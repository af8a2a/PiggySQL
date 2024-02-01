use std::{
    collections::HashSet,
    ops::{Bound, RangeBounds},
    sync::Arc,
    vec,
};

use serde::{Deserialize, Serialize};

use super::{
    engine::{StorageEngine, StorageEngineError},
    keycode::{encode_bytes, encode_u64, take_bytes, take_u64},
};
type Version = u64;

#[derive(Debug, Deserialize, Serialize)]
pub enum Key {
    /// The next available version.
    NextVersion,
    /// Active (uncommitted) transactions.
    TxnActive(Version),
    /// A snapshot of the active set at each version. Only written for
    /// versions where the active set is non-empty (excluding itself).
    TxnActiveSnapshot(Version),
    /// Keeps track of all keys written to by an active transaction (identified
    /// by its version), in case it needs to roll back.   
    /// Write set for a transaction version.
    TxnWrite(Version, Vec<u8>),
    /// A versioned key/value pair.
    Version(Vec<u8>, Version),
    /// Unversioned non-transactional key/value pairs. These exist separately
    /// from versioned keys, i.e. the unversioned key "foo" is entirely
    /// independent of the versioned key "foo@7". These are mostly used
    /// for metadata.
    Unversioned(Vec<u8>),
}
/// MVCC key prefixes, for prefix scans. These must match the keys above,
/// including the enum variant index.
#[derive(Debug, Deserialize, Serialize)]
enum KeyPrefix {
    NextVersion,
    TxnActive,
    TxnActiveSnapshot,
    TxnWrite(Version),
    Version(Vec<u8>),
    Unversioned,
}
impl KeyPrefix {
    fn encode(&self) -> Result<Vec<u8>, MVCCError> {
        match self {
            KeyPrefix::NextVersion => Ok(vec![0x01]),
            KeyPrefix::TxnActive => Ok(vec![0x02]),
            KeyPrefix::TxnActiveSnapshot => Ok(vec![0x03]),
            KeyPrefix::TxnWrite(version) => Ok([&[0x04][..], &encode_u64(*version)].concat()),
            KeyPrefix::Version(key) => Ok([&[0x05][..], &encode_bytes(&key)].concat()),
            KeyPrefix::Unversioned => Ok(vec![0x06]),
        }
    }
}

impl Key {
    pub fn decode(bytes: &[u8]) -> Result<Self, MVCCError> {
        Ok(match bytes[0] {
            0x01 => Self::NextVersion,
            0x02 => Self::TxnActive(take_u64(bytes)?),
            0x03 => Self::TxnActiveSnapshot(take_u64(bytes)?),
            0x04 => Self::TxnWrite(take_u64(bytes)?, take_bytes(bytes)?.into()),
            0x05 => Self::Version(take_bytes(bytes)?.into(), take_u64(bytes)?),
            0x06 => Self::Unversioned(take_bytes(bytes)?.into()),
            _ => {
                return Err(MVCCError::Serialization(format!(
                    "Invalid key prefix {:?}",
                    bytes[0]
                )))
            }
        })
    }

    pub fn encode(&self) -> Result<Vec<u8>, MVCCError> {
        match self {
            Key::NextVersion => Ok(vec![0x01]),
            Key::TxnActive(version) => Ok([&[0x02][..], &encode_u64(*version)].concat()),
            Key::TxnActiveSnapshot(version) => Ok([&[0x03][..], &encode_u64(*version)].concat()),
            Key::TxnWrite(version, key) => {
                Ok([&[0x04][..], &encode_u64(*version), &encode_bytes(&key)].concat())
            }
            Key::Version(key, version) => {
                Ok([&[0x05][..], &encode_bytes(&key), &encode_u64(*version)].concat())
            }
            Key::Unversioned(key) => Ok([&[0x06][..], &encode_bytes(&key)].concat()),
        }
    }
}

pub struct MVCCTransaction<E: StorageEngine> {
    engine: Arc<E>,
    state: TransactionState,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct TransactionState {
    pub version: Version,
    pub read_only: bool,
    pub active: HashSet<Version>,
}
impl TransactionState {
    fn is_visible(&self, version: Version) -> bool {
        if self.active.contains(&version) {
            false
        } else if self.read_only {
            version < self.version
        } else {
            version <= self.version
        }
    }
}
impl<E: StorageEngine> MVCCTransaction<E> {
    pub fn begin(engine: Arc<E>) -> Result<MVCCTransaction<E>, MVCCError> {
        let version = match engine.get(&Key::NextVersion.encode()?)? {
            Some(ref v) => take_u64(v)?,
            None => 1,
        };
        engine.set(&Key::NextVersion.encode()?, encode_u64(version + 1).into())?;
        let active = Self::scan_active(&engine)?;
        //创建这个事务运行时的快照
        if !active.is_empty() {
            engine.set(
                &Key::TxnActiveSnapshot(version).encode()?,
                bincode::serialize(&active)?,
            )?
        }
        engine.set(&Key::TxnActive(version).encode()?, vec![])?;
        Ok(Self {
            engine,
            state: TransactionState {
                version,
                read_only: false,
                active,
            },
        })
    }
    
    pub fn set(&self, key: &[u8], value: Vec<u8>) -> Result<(), MVCCError> {
        self.write_version(key, Some(value))
    }

    pub fn read_only(&self) -> bool {
        self.state.read_only
    }
    pub fn commit(self) -> Result<(), MVCCError> {
        if self.state.read_only {
            return Ok(());
        }
        let remove = self
            .engine
            .scan_prefix(&KeyPrefix::TxnWrite(self.state.version).encode()?)
            .map(|r| r.map(|(k, _)| k).expect("key should not be empty"))
            .collect::<Vec<_>>();
        for key in remove {
            self.engine.delete(&key)?;
        }

        self.engine
            .delete(&Key::TxnActive(self.state.version).encode()?)
            .map_err(|e| MVCCError::from(e))
    }
    pub fn rollback(self) -> Result<(), MVCCError> {
        if self.state.read_only {
            return Ok(());
        }
        //回滚最新事务的所有修改
        let mut rollback = Vec::new();
        let mut scan = self
            .engine
            .scan_prefix(&KeyPrefix::TxnWrite(self.state.version).encode()?);
        while let Some((key, _)) = scan.next().transpose()? {
            match Key::decode(&key)? {
                Key::TxnWrite(_, key) => {
                    rollback.push(Key::Version(key, self.state.version).encode()?)
                    // the version
                }
                key => {
                    return Err(MVCCError::KeyError(format!(
                        "Expected TxnWrite, got {:?}",
                        key
                    )))
                }
            };
            rollback.push(key); // the TxnWrite record
        }
        drop(scan);
        for key in rollback.into_iter() {
            self.engine.delete(&key)?;
        }
        self.engine
            .delete(&Key::TxnActive(self.state.version).encode()?)
            .map_err(|e| MVCCError::from(e)) // remove from active set
    }

    fn write_version(&self, key: &[u8], value: Option<Vec<u8>>) -> Result<(), MVCCError> {
        if self.state.read_only {
            return Err(MVCCError::Internal("Write in read only mode".into()));
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
        .encode()?;
        let to = Key::Version(key.into(), u64::MAX).encode()?;
        if let Some((key, _)) = self.engine.scan(from..=to).last().transpose()? {
            match Key::decode(&key)? {
                Key::Version(_, version) => {
                    //被不可见事务修改
                    if !self.state.is_visible(version) {
                        return Err(MVCCError::Internal("Write conflict".into()));
                    }
                }
                key => {
                    return Err(MVCCError::Internal(format!(
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
            vec![],
        )?;

        self.engine
            .set(
                &Key::Version(key.into(), self.state.version).encode()?,
                bincode::serialize(&value)?,
            )
            .map_err(|e| MVCCError::from(e))
    }

    pub fn scan(&self, start:Bound<Vec<u8>>,end:Bound<Vec<u8>>) -> Result<Scan<E>, MVCCError> {
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

    /// 向存储引擎写入tombstone.
    pub fn delete(&self, key: &[u8]) -> Result<(), MVCCError> {
        self.write_version(key, None)
    }

    fn scan_active(session: &Arc<E>) -> Result<HashSet<Version>, MVCCError> {
        let mut active = HashSet::new();
        let mut scan = session.scan_prefix(&KeyPrefix::TxnActive.encode()?);
        while let Some((key, _)) = scan.next().transpose()? {
            match Key::decode(&key)? {
                Key::TxnActive(version) => active.insert(version),
                _ => {
                    return Err(MVCCError::KeyError(format!(
                        "Expected TxnActive key, got {:?}",
                        key
                    )))
                }
            };
        }
        Ok(active)
    }

    ///查找最新数据
    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, MVCCError> {
        let from = Key::Version(key.into(), 0).encode()?;
        let to = Key::Version(key.into(), self.state.version).encode()?;
        let mut scan = self.engine.scan(from..=to).rev();
        //从新到旧遍历，找到第一个自己能够看见的版本
        while let Some((key, value)) = scan.next().transpose()? {
            match Key::decode(&key)? {
                Key::Version(_, version) => {
                    if self.state.is_visible(version) {
                        return Some(bincode::deserialize(&value))
                            .transpose()
                            .map_err(|e| MVCCError::from(e));
                    }
                }
                key => {
                    return Err(MVCCError::Internal(format!(
                        "Expected Key::Version got {:?}",
                        key
                    )))
                }
            };
        }
        Ok(None)
    }
}

struct VersionIterator<'a, E: StorageEngine + 'a> {
    txn: &'a TransactionState,
    inner: E::ScanIterator<'a>,
}

impl<'a, E: StorageEngine + 'a> VersionIterator<'a, E> {
    fn new(txn: &'a TransactionState, inner: E::ScanIterator<'a>) -> Self {
        Self { txn, inner }
    }
    /// Decodes a raw engine key into an MVCC key and version, returning None if
    /// the version is not visible.
    fn decode_visible(&self, key: &[u8]) -> Result<Option<(Vec<u8>, Version)>, MVCCError> {
        let (key, version) = match Key::decode(key)? {
            Key::Version(key, version) => (key.to_vec(), version),
            key => {
                return Err(MVCCError::KeyError(format!(
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
    fn try_next(&mut self) -> Result<Option<(Vec<u8>, Version, Vec<u8>)>, MVCCError> {
        while let Some((key, value)) = self.inner.next().transpose()? {
            if let Some((key, version)) = self.decode_visible(&key)? {
                return Ok(Some((key, version, value)));
            }
        }
        Ok(None)
    }
    // Fallible next_back(), emitting the previous item, or None if exhausted.
    fn try_next_back(&mut self) -> Result<Option<(Vec<u8>, Version, Vec<u8>)>, MVCCError> {
        while let Some((key, value)) = self.inner.next_back().transpose()? {
            if let Some((key, version)) = self.decode_visible(&key)? {
                return Ok(Some((key, version, value)));
            }
        }
        Ok(None)
    }
}

impl<'a, E: StorageEngine> Iterator for VersionIterator<'a, E> {
    type Item = Result<(Vec<u8>, Version, Vec<u8>), MVCCError>;
    fn next(&mut self) -> Option<Self::Item> {
        self.try_next().transpose()
    }
}

impl<'a, E: StorageEngine> DoubleEndedIterator for VersionIterator<'a, E> {
    fn next_back(&mut self) -> Option<Self::Item> {
        self.try_next_back().transpose()
    }
}

#[derive(Clone, Debug)]
pub struct MVCC<E: StorageEngine> {
    engine: Arc<E>,
}
impl<E: StorageEngine> MVCC<E> {
    pub fn new(engine: Arc<E>) -> Self {
        Self { engine }
    }
    pub fn begin(&self) -> Result<MVCCTransaction<E>, MVCCError> {
        MVCCTransaction::begin(self.engine.clone())
    }

    pub fn get_unversioned(&self, key: &[u8]) -> Result<Option<Vec<u8>>, MVCCError> {
        self.engine
            .get(&Key::Unversioned(key.to_vec()).encode()?)
            .map_err(|e| MVCCError::from(e))
    }
    pub fn set_unversioned(&self, key: &[u8], value: Vec<u8>) -> Result<(), MVCCError> {
        self.engine
            .set(&Key::Unversioned(key.into()).encode()?, value)
            .map_err(|e| MVCCError::from(e))
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
    pub fn iter(&mut self) -> ScanIterator<'_, E> {
        let inner = match &self.param {
            ScanType::Range(range) => self.engine.scan(range.clone()),
            ScanType::Prefix(prefix) => self.engine.scan_prefix(prefix),
        };
        ScanIterator::new(self.txn, inner)
    }

    /// Collects the result to a vector.
    pub fn to_vec(&mut self) -> Result<Vec<(Vec<u8>, Vec<u8>)>, MVCCError> {
        self.iter().collect()
    }
}
pub struct ScanIterator<'a, E: StorageEngine + 'a> {
    /// Decodes and filters visible MVCC versions from the inner engine iterator.
    inner: std::iter::Peekable<VersionIterator<'a, E>>,
    /// The previous key emitted by try_next_back(). Note that try_next() does
    /// not affect reverse positioning: double-ended iterators consume from each
    /// end independently.
    last_back: Option<Vec<u8>>,
}
impl<'a, E: StorageEngine + 'a> ScanIterator<'a, E> {
    /// Creates a new scan iterator.
    fn new(txn: &'a TransactionState, inner: E::ScanIterator<'a>) -> Self {
        Self {
            inner: VersionIterator::new(txn, inner).peekable(),
            last_back: None,
        }
    }

    /// Fallible next(), emitting the next item, or None if exhausted.
    fn try_next(&mut self) -> Result<Option<(Vec<u8>, Vec<u8>)>, MVCCError> {
        while let Some((key, _version, value)) = self.inner.next().transpose()? {
            // If the next key equals this one, we're not at the latest version.
            match self.inner.peek() {
                Some(Ok((next, _, _))) if next == &key => continue,
                Some(Err(err)) => return Err(err.clone()),
                Some(Ok(_)) | None => {}
            }
            // If the key is live (not a tombstone), emit it.
            if let Some(value) = bincode::deserialize(&value)? {
                return Ok(Some((key, value)));
            }
        }
        Ok(None)
    }
    /// Fallible next_back(), emitting the next item from the back, or None if
    /// exhausted.
    fn try_next_back(&mut self) -> Result<Option<(Vec<u8>, Vec<u8>)>, MVCCError> {
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
            if let Some(value) = bincode::deserialize(&value)? {
                return Ok(Some((key, value)));
            }
        }
        Ok(None)
    }
}

impl<'a, E: StorageEngine> Iterator for ScanIterator<'a, E> {
    type Item = Result<(Vec<u8>, Vec<u8>), MVCCError>;
    fn next(&mut self) -> Option<Self::Item> {
        self.try_next().transpose()
    }
}

impl<'a, E: StorageEngine> DoubleEndedIterator for ScanIterator<'a, E> {
    fn next_back(&mut self) -> Option<Self::Item> {
        self.try_next_back().transpose()
    }
}

#[derive(Debug, Clone)]
pub enum MVCCError {
    InvalidVersion(String),
    EncodeError(String),
    StorageError(String),
    Internal(String),
    KeyError(String),
    Serialization(String),
}
impl From<bincode::Error> for MVCCError {
    fn from(e: bincode::Error) -> Self {
        MVCCError::EncodeError(format!("{:?}", e))
    }
}
impl From<StorageEngineError> for MVCCError {
    fn from(e: StorageEngineError) -> Self {
        MVCCError::StorageError(format!("{:?}", e))
    }
}
impl From<std::array::TryFromSliceError> for MVCCError {
    fn from(err: std::array::TryFromSliceError) -> Self {
        MVCCError::Serialization(err.to_string())
    }
}

impl From<std::string::FromUtf8Error> for MVCCError {
    fn from(err: std::string::FromUtf8Error) -> Self {
        MVCCError::Serialization(err.to_string())
    }
}
