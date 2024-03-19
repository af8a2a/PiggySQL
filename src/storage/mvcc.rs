mod keycode;
pub mod lock_manager;
use std::{
    collections::HashSet,
    ops::{Bound, Sub},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    vec,
};

use self::lock_manager::LockManager;

use super::engine::{KvScan, StorageEngine};
use crate::{errors::*, CONFIG_MAP};

use futures::executor::block_on;
pub use keycode::*;

use serde::{Deserialize, Serialize};
type Version = u64;
use lazy_static::lazy_static;
use tracing::{debug, info, trace};
lazy_static! {
    static ref LOCK_MANAGER: Arc<LockManager> = Arc::new(LockManager::new());
}
/// Serializes MVCC metadata.
fn serialize<V: Serialize>(value: &V) -> Result<Vec<u8>> {
    Ok(bincode::serialize(value)?)
}

/// Deserializes MVCC metadata.
fn deserialize<'a, V: Deserialize<'a>>(bytes: &'a [u8]) -> Result<V> {
    Ok(bincode::deserialize(bytes)?)
}
const GC_BAIS:u64=1;
pub struct MVCCTransaction<E: StorageEngine> {
    engine: Arc<E>,
    pub(crate) lock_manager: Option<Arc<LockManager>>,
    state: TransactionState,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct TransactionState {
    pub version: Version,
    pub active: HashSet<Version>,
}
impl TransactionState {
    fn is_visible(&self, version: Version) -> bool {
        if self.active.contains(&version) {
            false
        } else {
            version <= self.version
        }
    }
}
impl<E: StorageEngine> MVCCTransaction<E> {
    pub fn begin(engine: Arc<E>) -> Result<MVCCTransaction<E>> {
        let version = match engine.get(&Key::NextVersion.encode()?)? {
            Some(ref v) => deserialize(v)?,
            None => 1,
        };
        engine.set(&Key::NextVersion.encode()?, serialize(&(version + 1))?)?;

        let active = Self::scan_active(&engine)?;
        //创建这个事务运行时的快照
        if !active.is_empty() {
            engine.set(
                &Key::TxnActiveSnapshot(version).encode()?,
                serialize(&active)?,
            )?
        }
        //设置活跃事务
        engine.set(&Key::TxnActive(version).encode()?, vec![0])?;
        // if let Some(ref lock_manager) = lock_manager {
        //     lock_manager.init_txn(version);
        // }

        Ok(Self {
            engine,
            state: TransactionState { version, active },
            lock_manager: None,
        })
    }
    pub fn set_serializable(&mut self, serializable: bool) {
        if serializable {
            self.lock_manager = Some(LOCK_MANAGER.clone());
            if let Some(ref lock_manager) = self.lock_manager {
                lock_manager.init_txn(self.state.version);
            }
        } else {
            self.lock_manager = None;
        }
    }
    pub fn set(&self, key: &[u8], value: Vec<u8>) -> Result<()> {
        self.write_version(key, Some(value))
    }

    pub fn commit(self) -> Result<()> {
        if let Some(lock_manager) = &self.lock_manager {
            lock_manager.check_abort(self.state.version)?;
            let commit_timestamp = match self.engine.get(&Key::NextVersion.encode()?)? {
                Some(ref v) => deserialize(v)?,
                None => 1,
            };
            lock_manager.commit_txn(self.state.version, commit_timestamp)?;
        }

        let remove = self
            .engine
            .scan_prefix(&KeyPrefix::TxnWrite(self.state.version).encode()?)?
            .map(|r| r.map(|(k, _)| k).expect("key should not be empty"))
            .collect::<Vec<_>>();
        for key in remove {
            self.engine.delete(&key)?;
        }
        self.engine
            .delete(&Key::TxnActive(self.state.version).encode()?)
    }
    pub fn rollback(self) -> Result<()> {
        if let Some(lock_manager) = &self.lock_manager {
            lock_manager.rollback_txn(self.state.version);
        }

        //回滚最新事务的所有修改
        let mut rollback = Vec::new();
        let mut scan = self
            .engine
            .scan_prefix(&KeyPrefix::TxnWrite(self.state.version).encode()?)?;
        while let Some((key, _)) = scan.next().transpose()? {
            trace!("rollback {:?}", key);
            match Key::decode(&key)? {
                Key::TxnWrite(_, key) => {
                    rollback.push(Key::Version(key, self.state.version).encode()?)
                    // the version
                }
                _=>{},
                // key => {
                //     return Err(DatabaseError::InternalError(format!(
                //         "Expected TxnWrite, got {:?}",
                //         key
                //     )))
                // }
            };
            rollback.push(key); // the TxnWrite record
        }
        drop(scan);
        for key in rollback.into_iter() {
            self.engine.delete(&key)?;
        }
        self.engine
            .delete(&Key::TxnActive(self.state.version).encode()?)
    }

    fn write_version(&self, key: &[u8], value: Option<Vec<u8>>) -> Result<()> {
        if let Some(lock_manager) = &self.lock_manager {
            lock_manager.acquire_write_lock(key.to_vec(), self.state.version);
            lock_manager.check_read_locks(key.to_vec(), self.state.version)?;
        }

        // Check for write conflicts, i.e. if the latest key is invisible to us
        // (either a newer version, or an uncommitted version in our past). We
        // can only conflict with the latest key, since all transactions enforce
        // the same invariant.
        let from = Key::Version(
            key.into(),
            self.state
                .active
                .iter()
                .min()
                .copied()
                .unwrap_or(self.state.version + 1),
        )
        .encode()?; //活跃事务中最老的版本或下一个版本
        let to = Key::Version(key.into(), u64::MAX).encode()?;

        if let Some((key, _)) = self.engine.scan(from..=to)?.last().transpose()? {
            //检查key是否被其他事务修改
            //在写写冲突判断中,出现更新的版本要么是自己写入的,要么是被其它不可见事务写入,也就是活跃事务
            match Key::decode(&key)? {
                Key::Version(_, version) => {
                    //被其它活跃事务修改
                    if !self.state.is_visible(version) {
                        //W-W冲突
                        return Err(DatabaseError::Serialization);
                    }
                }
                key => {
                    return Err(DatabaseError::InternalError(format!(
                        "Expected Key::Version got {:?}",
                        key
                    )))
                }
            }
        }

        // Write the new version and its write record.
        //
        // NB: TxnWrite contains the provided user key, not the encoded engine
        // key, since we can construct the engine key using the version.
        self.engine.set(
            &Key::TxnWrite(self.state.version, key.into()).encode()?,
            vec![0],
        )?;
        self.engine.set(
            &Key::Version(key.into(), self.state.version).encode()?,
            serialize(&value)?,
        )
    }

    pub fn scan(&self, start: Bound<Vec<u8>>, end: Bound<Vec<u8>>) -> Result<Scan<E>> {
        let start = match start {
            Bound::Excluded(k) => Bound::Excluded(Key::Version(k.clone(), u64::MAX).encode()?),
            Bound::Included(k) => Bound::Included(Key::Version(k.clone(), 0).encode()?),
            Bound::Unbounded => Bound::Included(Key::Version(vec![], 0).encode()?),
        };
        let end = match end {
            Bound::Excluded(k) => Bound::Excluded(Key::Version(k.clone(), 0).encode()?),
            Bound::Included(k) => Bound::Included(Key::Version(k.clone(), u64::MAX).encode()?),
            Bound::Unbounded => Bound::Excluded(KeyPrefix::Unversioned.encode()?),
        };
        Ok(Scan::from_range(
            self.engine.clone(),
            &self.state,
            start,
            end,
        ))
    }

    pub fn scan_prefix(&self, prefix: &[u8]) -> Result<Scan<E>> {
        // Normally, KeyPrefix::Version will only match all versions of the
        // exact given key. We want all keys maching the prefix, so we chop off
        // the KeyCode byte slice terminator 0x0000 at the end.
        let mut prefix = KeyPrefix::Version(prefix.into()).encode()?;
        prefix.truncate(prefix.len() - 2);
        Ok(Scan::from_prefix(self.engine.clone(), &self.state, prefix))
    }

    /// 向存储引擎写入tombstone.
    pub fn delete(&self, key: &[u8]) -> Result<()> {
        self.write_version(key, None)
    }

    fn scan_active(session: &Arc<E>) -> Result<HashSet<Version>> {
        let mut active = HashSet::new();
        let mut scan = session.scan_prefix(&KeyPrefix::TxnActive.encode()?)?;
        while let Some((key, _)) = scan.next().transpose()? {
            match Key::decode(&key)? {
                Key::TxnActive(version) => active.insert(version),
                _ => {
                    return Err(DatabaseError::InternalError(format!(
                        "Expected TxnActive key, got {:?}",
                        key
                    )))
                }
            };
        }
        Ok(active)
    }

    ///查找最新数据
    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        // Acquires the SIREAD lock and records RW-dependencies with other writers.
        if let Some(lock_manager) = &self.lock_manager {
            lock_manager.acquire_read_lock(key.to_vec(), self.state.version);
            lock_manager.check_write_locks(key.to_vec(), self.state.version)?;
        }

        let from = Key::Version(key.into(), 0).encode()?;
        let to = Key::Version(key.into(), self.state.version).encode()?;

        let mut scan = self.engine.scan(from..=to)?.rev();
        //从新到旧遍历，找到第一个自己能够看见的版本
        while let Some((key, value)) = scan.next().transpose()? {
            match Key::decode(&key)? {
                Key::Version(_, version) => {
                    if self.state.is_visible(version) {
                        return deserialize(&value);
                    }
                }
                key => {
                    return Err(DatabaseError::InternalError(format!(
                        "Expected Key::Version got {:?}",
                        key
                    )))
                }
            };
        }
        let from = Key::Version(key.into(), self.state.version + 1).encode()?;
        let to = Key::Version(key.into(), std::u64::MAX).encode()?;

        // Records RW-dependencies with the creators of newer-versioned entries.
        if let Some(lock_manager) = &self.lock_manager {
            let mut scan = self.engine.scan(from..=to)?;
            while let Some((_k, _)) = scan.next().transpose()? {
                match Key::decode(key)? {
                    Key::Version(_, version) => {
                        lock_manager.abort_or_record_conflict(version, self.state.version)?
                    }
                    k => {
                        return Err(DatabaseError::InternalError(format!(
                            "Expected Txn::Record, got {:?}",
                            k
                        )))
                    }
                }
            }
        }

        Ok(None)
    }
}

struct VersionIterator<'a> {
    txn: &'a TransactionState,
    inner: KvScan,
}

impl<'a> VersionIterator<'a> {
    fn new(txn: &'a TransactionState, inner: KvScan) -> Self {
        Self { txn, inner }
    }
    /// Decodes a raw engine key into an MVCC key and version, returning None if
    /// the version is not visible.
    fn decode_visible(&self, key: &[u8]) -> Result<Option<(Vec<u8>, Version)>> {
        let (key, version) = match Key::decode(key)? {
            Key::Version(key, version) => (key.to_vec(), version),
            key => {
                return Err(DatabaseError::InternalError(format!(
                    "Expected Key::Version got {:?}",
                    key
                )))
            }
        };
        if self.txn.is_visible(version) {
            Ok(Some((key, version)))
        } else {
            Ok(None)
        }
    }
    // Fallible next(), emitting the next item, or None if exhausted.
    fn try_next(&mut self) -> Result<Option<(Vec<u8>, Version, Vec<u8>)>> {
        while let Some((key, value)) = self.inner.next().transpose()? {
            if let Some((key, version)) = self.decode_visible(&key)? {
                return Ok(Some((key, version, value)));
            }
        }
        Ok(None)
    }
    // Fallible next_back(), emitting the previous item, or None if exhausted.
    fn try_next_back(&mut self) -> Result<Option<(Vec<u8>, Version, Vec<u8>)>> {
        while let Some((key, value)) = self.inner.next_back().transpose()? {
            if let Some((key, version)) = self.decode_visible(&key)? {
                return Ok(Some((key, version, value)));
            }
        }
        Ok(None)
    }
}

impl<'a> Iterator for VersionIterator<'a> {
    type Item = Result<(Vec<u8>, Version, Vec<u8>)>;
    fn next(&mut self) -> Option<Self::Item> {
        self.try_next().transpose()
    }
}

impl<'a> DoubleEndedIterator for VersionIterator<'a> {
    fn next_back(&mut self) -> Option<Self::Item> {
        self.try_next_back().transpose()
    }
}

#[derive(Clone)]
pub struct MVCC<E: StorageEngine> {
    engine: Arc<E>,
    last_gc: Arc<AtomicU64>,
    watermark: Arc<AtomicU64>,
    threshold: u64,
}
impl<E: StorageEngine> MVCC<E> {
    pub fn new(engine: Arc<E>) -> Self {
        let threshold=CONFIG_MAP.get("gc_threshold").unwrap().parse::<u64>().unwrap();
        let mvcc = MVCC {
            engine,
            watermark: Arc::new(AtomicU64::new(1)),
            last_gc: Arc::new(AtomicU64::new(0)),
            threshold,
        };
        mvcc.do_recovery().unwrap();
        mvcc
    }

    pub async fn begin(&self) -> Result<MVCCTransaction<E>> {
        self.watermark.fetch_add(1, Ordering::SeqCst);
        if self
            .watermark
            .load(Ordering::SeqCst)
            .sub(self.last_gc.load(Ordering::SeqCst))
            .gt(&self.threshold)
        {
            let watermark = self.watermark.load(Ordering::SeqCst);
            self.last_gc.store(watermark, Ordering::SeqCst);

            info!("GC watermark: {}", watermark);
            // self.gc()?;
            self.gc()?;
            // self.engine.flush()?;
        }
        MVCCTransaction::begin(self.engine.clone())
    }
    pub fn do_recovery(&self) -> Result<()> {
        self.gc()?;

        let active_set = self
            .engine
            .scan_prefix(&KeyPrefix::TxnActive.encode()?)?
            .collect::<Result<Vec<_>>>()?;
        let write_set = self
            .engine
            .scan(&KeyPrefix::TxnWrite(0).encode()?..&KeyPrefix::TxnWrite(u64::MAX).encode()?)?
            .collect::<Result<Vec<_>>>()?;
        for (write_key, _) in write_set {
            let key = Key::decode(&write_key)?;
            if let Key::TxnWrite(_, key) = key {
                self.engine.delete(&key)?;
            }
        }
        for (active_key, _) in active_set {
            self.engine.delete(&active_key)?;
        }
        Ok(())
    }

    pub fn gc(&self) -> Result<()> {
        let mut scan = self
            .engine
            .scan(&Key::Version(vec![], 0).encode()?..&KeyPrefix::Unversioned.encode()?)?
            .rev();
        info!("start MVCC GC!");
        let mut gc_count = 0;
        let netx_version = match self.engine.get(&Key::NextVersion.encode()?)? {
            Some(ref v) => deserialize(v)?,
            None => 1,
        };
        let active = MVCCTransaction::scan_active(&self.engine)?;
        let oldest_version = u64::max(*active.iter().min().unwrap_or(&netx_version)-GC_BAIS,1);
        let resume_state = TransactionState {
            version: oldest_version,
            active,
        };
        let mut mark=HashSet::new();
        let mut batch=Vec::new();
        while let Some((key, _)) = scan.next().transpose()? {
            match Key::decode(&key)? {
                Key::Version(key, version) => {
                    //删除过时的键值对
                    //注意:现存最早的事务能看到的记录不能被删除
                    if resume_state.is_visible(version)&&!mark.contains(&key) {
                        mark.insert(key.clone());
                    }else if resume_state.is_visible(version)&&mark.contains(&key){
                        gc_count+=1;
                        batch.push((key, version));
                    }
                }
                _ => {
                    //nothing to do
                }
            }
        }
        for (key,version) in batch{
            self.engine.delete(&Key::Version(key, version).encode()?)?;

        }
        info!("clean {} keys", gc_count);
        Ok(())
    }
}

pub struct Scan<'a, E: StorageEngine + 'a> {
    /// Access to the locked engine.
    engine: Arc<E>,
    /// The transaction state.
    txn: &'a TransactionState,
    /// The scan type and parameter.
    param: ScanType,
}
enum ScanType {
    Range((Bound<Vec<u8>>, Bound<Vec<u8>>)),
    Prefix(Vec<u8>),
}

impl<'a, E: StorageEngine + 'a> Scan<'a, E> {
    /// Runs a normal range scan.
    fn from_range(
        engine: Arc<E>,
        txn: &'a TransactionState,
        start: Bound<Vec<u8>>,
        end: Bound<Vec<u8>>,
    ) -> Self {
        Self {
            engine,
            txn,
            param: ScanType::Range((start, end)),
        }
    }

    /// Runs a prefix scan.
    fn from_prefix(engine: Arc<E>, txn: &'a TransactionState, prefix: Vec<u8>) -> Self {
        Self {
            engine,
            txn,
            param: ScanType::Prefix(prefix),
        }
    }

    /// Returns an iterator over the result.
    pub fn iter(&mut self) -> ScanIterator<'_> {
        let inner = match &self.param {
            ScanType::Range(range) => self.engine.scan(range.clone()).unwrap(),
            ScanType::Prefix(prefix) => self.engine.scan_prefix(prefix).unwrap(),
        };

        ScanIterator::new(self.txn, inner)
    }

    /// Collects the result to a vector.
    pub fn to_vec(&mut self) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        self.iter().collect()
    }
}
pub struct ScanIterator<'a> {
    /// Decodes and filters visible MVCC versions from the inner engine iterator.
    inner: std::iter::Peekable<VersionIterator<'a>>,
    /// The previous key emitted by try_next_back(). Note that try_next() does
    /// not affect reverse positioning: double-ended iterators consume from each
    /// end independently.
    last_back: Option<Vec<u8>>,
}
impl<'a> ScanIterator<'a> {
    /// Creates a new scan iterator.
    fn new(txn: &'a TransactionState, inner: KvScan) -> Self {
        Self {
            inner: VersionIterator::new(txn, inner).peekable(),
            last_back: None,
        }
    }

    /// Fallible next(), emitting the next item, or None if exhausted.
    fn try_next(&mut self) -> Result<Option<(Vec<u8>, Vec<u8>)>> {
        while let Some((key, _version, value)) = self.inner.next().transpose()? {
            // If the next key equals this one, we're not at the latest version.
            match self.inner.peek() {
                Some(Ok((next, _, _))) if next == &key => continue,
                Some(Err(err)) => return Err(DatabaseError::InternalError(err.to_string())),
                Some(Ok(_)) | None => {}
            }
            // If the key is live (not a tombstone), emit it.
            if let Some(value) = deserialize(&value)? {
                return Ok(Some((key, value)));
            }
        }
        Ok(None)
    }
    /// Fallible next_back(), emitting the next item from the back, or None if
    /// exhausted.
    fn try_next_back(&mut self) -> Result<Option<(Vec<u8>, Vec<u8>)>> {
        while let Some((key, _version, value)) = self.inner.next_back().transpose()? {
            // If this key is the same as the last emitted key from the back,
            // this must be an older version, so skip it.
            if let Some(last) = &self.last_back {
                if last == &key {
                    continue;
                }
            }
            self.last_back = Some(key.clone());

            // If the key is live (not a tombstone), emit it.
            if let Some(value) = deserialize(&value)? {
                return Ok(Some((key, value)));
            }
        }
        Ok(None)
    }
}

impl<'a> Iterator for ScanIterator<'a> {
    type Item = Result<(Vec<u8>, Vec<u8>)>;
    fn next(&mut self) -> Option<Self::Item> {
        self.try_next().transpose()
    }
}

impl<'a> DoubleEndedIterator for ScanIterator<'a> {
    fn next_back(&mut self) -> Option<Self::Item> {
        self.try_next_back().transpose()
    }
}

#[cfg(test)]
pub mod tests {
    use std::collections::HashMap;

    use itertools::Itertools;

    use crate::storage::engine::memory::Memory;

    use super::*;
    /// Asserts that a scan yields the expected result.
    macro_rules! assert_scan {
        ( $scan:expr => { $( $key:expr => $value:expr),* $(,)? } ) => {
            let result = $scan.to_vec()?;
            let expect = vec![
                $( ($key.to_vec(), $value.to_vec()), )*
            ];
            assert_eq!(result, expect);
        };
    }

    impl<E: StorageEngine> MVCC<E> {
        async fn setup(&self, data: Vec<(&[u8], Version, Option<&[u8]>)>) -> Result<()> {
            // Segment the writes by version.
            let mut writes = HashMap::new();
            for (key, version, value) in data {
                writes
                    .entry(version)
                    .or_insert(Vec::new())
                    .push((key.to_vec(), value.map(|v| v.to_vec())));
            }
            // Insert the writes with individual transactions.
            for i in 1..=writes.keys().max().copied().unwrap_or(0) {
                let txn = self.begin().await?;
                for (key, value) in writes.get(&i).unwrap_or(&Vec::new()) {
                    if let Some(value) = value {
                        txn.set(key, value.clone())?;
                    } else {
                        txn.delete(key)?;
                    }
                }
                txn.commit()?;
            }
            Ok(())
        }
    }
    #[tokio::test]
    /// Tests that key prefixes are actually prefixes of keys.
    async fn key_prefix() -> Result<()> {
        let cases = vec![
            (KeyPrefix::NextVersion, Key::NextVersion),
            (KeyPrefix::TxnActive, Key::TxnActive(1)),
            (KeyPrefix::TxnActiveSnapshot, Key::TxnActiveSnapshot(1)),
            (
                KeyPrefix::TxnWrite(1),
                Key::TxnWrite(1, b"foo".as_slice().into()),
            ),
            (
                KeyPrefix::Version(b"foo".as_slice().into()),
                Key::Version(b"foo".as_slice().into(), 1),
            ),
            (
                KeyPrefix::Unversioned,
                Key::Unversioned(b"foo".as_slice().into()),
            ),
        ];

        for (prefix, key) in cases {
            let prefix = prefix.encode()?;
            let key = key.encode()?;
            assert_eq!(prefix, key[..prefix.len()])
        }
        Ok(())
    }

    #[tokio::test]
    /// Begin should create txns with new versions and current active sets.
    async fn begin() -> Result<()> {
        let mvcc = MVCC::new(Arc::new(Memory::new()));

        let t1 = mvcc.begin().await?;
        assert_eq!(
            t1.state,
            TransactionState {
                version: 1,

                active: HashSet::new()
            }
        );

        let t2 = mvcc.begin().await?;
        assert_eq!(
            t2.state,
            TransactionState {
                version: 2,
                active: HashSet::from([1])
            }
        );

        let t3 = mvcc.begin().await?;
        assert_eq!(
            t3.state,
            TransactionState {
                version: 3,

                active: HashSet::from([1, 2])
            }
        );

        t2.commit()?; // commit to remove from active set

        let t4 = mvcc.begin().await?;
        assert_eq!(
            t4.state,
            TransactionState {
                version: 4,

                active: HashSet::from([1, 3])
            }
        );

        Ok(())
    }
    #[tokio::test]
    /// Get should return the correct latest value.
    async fn get() -> Result<()> {
        let mvcc = MVCC::new(Arc::new(Memory::new()));
        let t1 = mvcc.begin().await?;
        t1.set(b"key", vec![1])?;
        t1.set(b"updated", vec![1])?;
        t1.set(b"updated", vec![2])?;

        t1.set(b"deleted", vec![1])?;
        t1.write_version(b"deleted", None)?;
        t1.write_version(b"tombstone", None)?;
        t1.commit()?;

        let mut t1 = mvcc.begin().await?;
        t1.set_serializable(true);
        assert_eq!(t1.get(b"key")?, Some(vec![1]));
        assert_eq!(t1.get(b"updated")?, Some(vec![2]));
        assert_eq!(t1.get(b"deleted")?, None);
        assert_eq!(t1.get(b"tombstone")?, None);

        Ok(())
    }
    #[tokio::test]
    /// Get should be isolated from future and uncommitted transactions.
    async fn get_isolation() -> Result<()> {
        let mvcc = MVCC::new(Arc::new(Memory::new()));

        let t1 = mvcc.begin().await?;
        t1.set(b"a", vec![1])?;
        t1.set(b"b", vec![1])?;
        t1.set(b"d", vec![1])?;
        t1.set(b"e", vec![1])?;
        t1.commit()?;

        let t2 = mvcc.begin().await?;
        t2.set(b"a", vec![2])?;
        t2.delete(b"b")?;
        t2.set(b"c", vec![2])?;

        let t3 = mvcc.begin().await?;

        let t4 = mvcc.begin().await?;
        t4.set(b"d", vec![3])?;
        t4.delete(b"e")?;
        t4.set(b"f", vec![3])?;
        t4.commit()?;

        assert_eq!(t3.get(b"a")?, Some(vec![1])); // uncommitted update
        assert_eq!(t3.get(b"b")?, Some(vec![1])); // uncommitted delete
        assert_eq!(t3.get(b"c")?, None); // uncommitted write
        assert_eq!(t3.get(b"d")?, Some(vec![1])); // future update
        assert_eq!(t3.get(b"e")?, Some(vec![1])); // future delete
        assert_eq!(t3.get(b"f")?, None); // future write

        Ok(())
    }
    #[tokio::test]
    // A fuzzy (or unrepeatable) read is when t2 sees a value change after t1
    // updates it. Snapshot isolation prevents this.
    async fn anomaly_fuzzy_read() -> Result<()> {
        let mvcc = MVCC::new(Arc::new(Memory::new()));
        mvcc.setup(vec![(b"key", 1, Some(&[0]))]).await?;
        //todo change test
        let t1 = mvcc.begin().await?;
        let t2 = mvcc.begin().await?;

        assert_eq!(t2.get(b"key")?, Some(vec![0]));
        t1.set(b"key", b"t1".to_vec())?;
        t1.commit()?;
        assert_eq!(t2.get(b"key")?, Some(vec![0]));

        Ok(())
    }

    #[tokio::test]
    // Read skew is when t1 reads a and b, but t2 modifies b in between the
    // reads. Snapshot isolation prevents this.
    async fn anomaly_read_skew() -> Result<()> {
        let mvcc = MVCC::new(Arc::new(Memory::new()));
        mvcc.setup(vec![(b"a", 1, Some(&[0])), (b"b", 1, Some(&[0]))])
            .await?;

        let t1 = mvcc.begin().await?;
        let t2 = mvcc.begin().await?;

        assert_eq!(t1.get(b"a")?, Some(vec![0]));
        t2.set(b"a", vec![2])?;
        t2.set(b"b", vec![2])?;
        t2.commit()?;
        assert_eq!(t1.get(b"a")?, Some(vec![0]));

        Ok(())
    }

    #[tokio::test]
    // Write skew is when t1 reads a and writes it to b while t2 reads b and
    // writes it to a. Snapshot isolation DOES NOT prevent this, which is
    // expected, so we assert the current behavior. Fixing this requires
    // implementing serializable snapshot isolation.
    async fn anomaly_write_skew() -> Result<()> {
        let mvcc = MVCC::new(Arc::new(Memory::new()));
        mvcc.setup(vec![(b"a", 1, Some(&[1])), (b"b", 1, Some(&[2]))])
            .await?;

        let mut t1 = mvcc.begin().await?;
        let mut t2 = mvcc.begin().await?;
        t1.set_serializable(true);
        t2.set_serializable(true);
        assert_eq!(t1.get(b"a")?, Some(vec![1]));
        assert_eq!(t2.get(b"b")?, Some(vec![2]));

        //write set has intersetion with read set
        assert_eq!(
            matches!(t1.set(b"b", vec![1]), Err(DatabaseError::Serialization)),
            false
        );
        assert_eq!(
            matches!(t2.set(b"a", vec![2]), Err(DatabaseError::Serialization)),
            true
        );

        assert_eq!(
            matches!(t1.commit(), Err(DatabaseError::Serialization)),
            true
        );
        assert_eq!(
            matches!(t2.commit(), Err(DatabaseError::Serialization)),
            true
        );

        Ok(())
    }
    #[tokio::test]
    /// Scan should be isolated from future and uncommitted transactions.
    async fn scan_isolation() -> Result<()> {
        let mvcc = MVCC::new(Arc::new(Memory::new()));

        let t1 = mvcc.begin().await?;
        t1.set(b"a", vec![1])?;
        t1.set(b"b", vec![1])?;
        t1.set(b"d", vec![1])?;
        t1.set(b"e", vec![1])?;
        t1.commit()?;

        let t2 = mvcc.begin().await?;
        t2.set(b"a", vec![2])?;
        t2.delete(b"b")?;
        t2.set(b"c", vec![2])?;

        let mut t3 = mvcc.begin().await?;
        t3.set_serializable(true);
        let t4 = mvcc.begin().await?;
        t4.set(b"d", vec![3])?;
        t4.delete(b"e")?;
        t4.set(b"f", vec![3])?;
        t4.commit()?;

        assert_scan!(t3.scan(Bound::Unbounded,Bound::Unbounded)? => {
            b"a" => [1], // uncommitted update
            b"b" => [1], // uncommitted delete
            // b"c" is uncommitted write
            b"d" => [1], // future update
            b"e" => [1], // future delete
            // b"f" is future write
        });

        Ok(())
    }
    #[tokio::test]
    /// Tests that the key encoding is resistant to key/version overlap.
    /// For example, a naïve concatenation of keys and versions would
    /// produce incorrect ordering in this case:
    ///
    // 00|00 00 00 00 00 00 00 01
    // 00 00 00 00 00 00 00 00 02|00 00 00 00 00 00 00 02
    // 00|00 00 00 00 00 00 00 03
    async fn scan_key_version_encoding() -> Result<()> {
        let mvcc = MVCC::new(Arc::new(Memory::new()));

        let t1 = mvcc.begin().await?;
        t1.set(&[0], vec![1])?;
        t1.commit()?;

        let t2 = mvcc.begin().await?;
        t2.set(&[0], vec![2])?;
        t2.set(&[0, 0, 0, 0, 0, 0, 0, 0, 2], vec![2])?;
        t2.commit()?;

        let t3 = mvcc.begin().await?;
        t3.set(&[0], vec![3])?;
        t3.commit()?;

        let mut t4 = mvcc.begin().await?;
        t4.set_serializable(true);
        assert_scan!(t4.scan(Bound::Unbounded,Bound::Unbounded)? => {
            b"\x00" => [3],
            b"\x00\x00\x00\x00\x00\x00\x00\x00\x02" => [2],
        });
        Ok(())
    }
    #[tokio::test]
    /// Sets should work on both existing, missing, and deleted keys, and be
    /// idempotent.
    async fn set() -> Result<()> {
        let mvcc = MVCC::new(Arc::new(Memory::new()));
        mvcc.setup(vec![(b"key", 1, Some(&[1])), (b"tombstone", 1, None)])
            .await?;

        let t1 = mvcc.begin().await?;
        t1.set(b"key", vec![2])?; // update
        t1.set(b"tombstone", vec![2])?; // update tombstone
        t1.set(b"new", vec![1])?; // new write
        t1.set(b"new", vec![1])?; // idempotent
        t1.set(b"new", vec![2])?; // update own
        t1.commit()?;

        Ok(())
    }

    #[tokio::test]
    /// Set should return serialization errors both for uncommitted versions
    /// (past and future), and future committed versions.
    async fn set_conflict() -> Result<()> {
        let mvcc = MVCC::new(Arc::new(Memory::new()));

        let t1 = mvcc.begin().await?;
        let t2 = mvcc.begin().await?;
        let t3 = mvcc.begin().await?;
        let t4 = mvcc.begin().await?;

        t1.set(b"a", vec![1])?;
        t3.set(b"c", vec![3])?;
        t4.set(b"d", vec![4])?;
        t4.commit()?;
        let _x = t2.set(b"a", vec![2]);

        assert_eq!(
            matches!(t2.set(b"a", vec![2]), Err(DatabaseError::Serialization)),
            true
        ); // past uncommitted
        assert_eq!(
            matches!(t2.set(b"c", vec![2]), Err(DatabaseError::Serialization)),
            true
        ); // future uncommitted
        assert_eq!(
            matches!(t2.set(b"d", vec![2]), Err(DatabaseError::Serialization)),
            true
        ); // future committed

        Ok(())
    }

    #[tokio::test]
    /// Tests that transaction rollback properly rolls back uncommitted writes,
    /// allowing other concurrent transactions to write the keys.
    async fn rollback() -> Result<()> {
        let mvcc = MVCC::new(Arc::new(Memory::new()));
        mvcc.setup(vec![
            (b"a", 1, Some(&[0])),
            (b"b", 1, Some(&[0])),
            (b"c", 1, Some(&[0])),
            (b"d", 1, Some(&[0])),
        ])
        .await?;

        // t2 will be rolled back. t1 and t3 are concurrent transactions.
        let t1 = mvcc.begin().await?;
        let t2 = mvcc.begin().await?;
        let t3 = mvcc.begin().await?;

        t1.set(b"a", vec![1])?;
        t2.set(b"b", vec![2])?;
        t2.delete(b"c")?;
        t3.set(b"d", vec![3])?;

        // Both t1 and t3 will get serialization errors with t2.
        assert!(matches!(
            t1.set(b"b", vec![1]),
            Err(DatabaseError::Serialization)
        ));
        assert!(matches!(
            t3.set(b"c", vec![3]),
            Err(DatabaseError::Serialization)
        ));

        // When t2 is rolled back, none of its writes will be visible, and t1
        // and t3 can perform their writes and successfully commit.
        t2.rollback()?;

        let mut t4 = mvcc.begin().await?;
        t4.set_serializable(true);
        assert_scan!(t4.scan(Bound::Unbounded,Bound::Unbounded)? => {
            b"a" => [0],
            b"b" => [0],
            b"c" => [0],
            b"d" => [0],
        });

        t1.set(b"b", vec![1])?;
        t3.set(b"c", vec![3])?;
        t1.commit()?;
        t3.commit()?;

        let mut t5 = mvcc.begin().await?;
        t5.set_serializable(true);
        assert_scan!(t5.scan(Bound::Unbounded,Bound::Unbounded)? => {
            b"a" => [1],
            b"b" => [1],
            b"c" => [3],
            b"d" => [3],
        });

        Ok(())
    }
    #[tokio::test]
    // A dirty write is when t2 overwrites an uncommitted value written by t1.
    // Snapshot isolation prevents this.
    async fn anomaly_dirty_write() -> Result<()> {
        let mvcc = MVCC::new(Arc::new(Memory::new()));

        let t1 = mvcc.begin().await?;
        t1.set(b"key", vec![1])?;

        let t2 = mvcc.begin().await?;
        assert!(matches!(
            t2.set(b"key", vec![2]),
            Err(DatabaseError::Serialization)
        ));

        Ok(())
    }

    #[tokio::test]
    // A dirty read is when t2 can read an uncommitted value set by t1.
    // Snapshot isolation prevents this.
    async fn anomaly_dirty_read() -> Result<()> {
        let mvcc = MVCC::new(Arc::new(Memory::new()));

        let t1 = mvcc.begin().await?;
        t1.set(b"key", vec![1])?;

        let t2 = mvcc.begin().await?;
        assert_eq!(t2.get(b"key")?, None);

        Ok(())
    }
    #[tokio::test]
    // A lost update is when t1 and t2 both read a value and update it, where
    // t2's update replaces t1. Snapshot isolation prevents this.
    async fn anomaly_lost_update() -> Result<()> {
        let mvcc = MVCC::new(Arc::new(Memory::new()));
        mvcc.setup(vec![(b"key", 1, Some(&[0]))]).await?;

        let mut t1 = mvcc.begin().await?;
        let mut t2 = mvcc.begin().await?;
        t1.set_serializable(true);
        t2.set_serializable(true);
        t1.get(b"key")?;
        t2.get(b"key")?;

        assert_eq!(
            matches!(t1.set(b"key", vec![1]), Err(DatabaseError::Serialization)),
            false
        );
        //in SSI islotation, t2 should get serialization error
        assert_eq!(
            matches!(t2.set(b"key", vec![2]), Err(DatabaseError::Serialization)),
            true
        );
        assert_eq!(
            matches!(t1.commit(), Err(DatabaseError::Serialization)),
            true
        );
        assert_eq!(
            matches!(t2.commit(), Err(DatabaseError::Serialization)),
            true
        );

        Ok(())
    }

    #[tokio::test]
    // A phantom read is when t1 reads entries matching some predicate, but a
    // modification by t2 changes which entries that match the predicate such
    // that a later read by t1 returns them. Snapshot isolation prevents this.
    //
    // We use a prefix scan as our predicate.
    async fn anomaly_phantom_read() -> Result<()> {
        let mvcc = MVCC::new(Arc::new(Memory::new()));
        mvcc.setup(vec![
            (b"a", 1, Some(&[0])),
            (b"ba", 1, Some(&[0])),
            (b"bb", 1, Some(&[0])),
        ])
        .await?;

        let t1 = mvcc.begin().await?;
        let t2 = mvcc.begin().await?;

        assert_scan!(t1.scan_prefix(b"b")? => {
            b"ba" => [0],
            b"bb" => [0],
        });

        t2.delete(b"ba")?;
        t2.set(b"bc", vec![2])?;
        t2.commit()?;

        assert_scan!(t1.scan_prefix(b"b")? => {
            b"ba" => [0],
            b"bb" => [0],
        });

        Ok(())
    }

    #[tokio::test]
    /// Tests MVCC GC.
    async fn gc() -> Result<()> {
        let mvcc = MVCC::new(Arc::new(Memory::new()));

        let t1 = mvcc.begin().await?;
        t1.set(b"a", vec![1])?;
        t1.commit()?;
        let t2 = mvcc.begin().await?;
        t2.set(b"a", vec![2])?;
        t2.commit()?;

        let t3 = mvcc.begin().await?;
        t3.set(b"a", vec![3])?;
        t3.commit()?;
        let scan = mvcc
            .engine
            .scan_prefix(&KeyPrefix::Version(b"a".to_vec()).encode()?)?
            .collect_vec();
        assert_eq!(scan.len(), 3);
        mvcc.gc()?;
        let scan = mvcc
            .engine
            .scan_prefix(&KeyPrefix::Version(b"a".to_vec()).encode()?)?
            .collect::<Result<Vec<_>>>()?;
        assert_eq!(scan.len(), 1);
        let t4 = mvcc.begin().await?;
        let kv = t4.get(b"a")?.unwrap();
        assert_eq!(kv, vec![3]);

        Ok(())
    }
}
