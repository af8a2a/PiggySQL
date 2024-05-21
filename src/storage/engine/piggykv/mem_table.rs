use std::ops::Bound;
use std::path::Path;
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;

use crate::errors::Result;
use bytes::Bytes;
use crossbeam_skiplist::map::Entry;
use crossbeam_skiplist::SkipMap;
use ouroboros::self_referencing;

use super::iterators::StorageIterator;
use super::key::{KeyBytes, KeySlice, TS_DEFAULT};
use super::table::SsTableBuilder;
use super::wal::Wal;


pub struct MemTable {
    pub(super) map: Arc<SkipMap<KeyBytes, Bytes>>,
    wal: Option<Wal>,
    id: usize,
    approximate_size: Arc<AtomicUsize>,
}

/// Create a bound of `Bytes` from a bound of `&[u8]`.
pub(crate) fn map_bound(bound: Bound<&[u8]>) -> Bound<Bytes> {
    match bound {
        Bound::Included(x) => Bound::Included(Bytes::copy_from_slice(x)),
        Bound::Excluded(x) => Bound::Excluded(Bytes::copy_from_slice(x)),
        Bound::Unbounded => Bound::Unbounded,
    }
}

/// Create a bound of `Bytes` from a bound of `KeySlice`.
pub(crate) fn map_key_bound(bound: Bound<KeySlice>) -> Bound<KeyBytes> {
    match bound {
        Bound::Included(x) => Bound::Included(KeyBytes::from_bytes_with_ts(
            Bytes::copy_from_slice(x.key_ref()),
            x.ts(),
        )),
        Bound::Excluded(x) => Bound::Excluded(KeyBytes::from_bytes_with_ts(
            Bytes::copy_from_slice(x.key_ref()),
            x.ts(),
        )),
        Bound::Unbounded => Bound::Unbounded,
    }
}

/// Create a bound of `Bytes` from a bound of `KeySlice`.
pub(crate) fn map_key_bound_plus_ts(bound: Bound<&[u8]>, ts: u64) -> Bound<KeySlice> {
    match bound {
        Bound::Included(x) => Bound::Included(KeySlice::from_slice(x, ts)),
        Bound::Excluded(x) => Bound::Excluded(KeySlice::from_slice(x, ts)),
        Bound::Unbounded => Bound::Unbounded,
    }
}

impl MemTable {
    /// Create a new mem-table.
    pub fn create(id: usize) -> Self {
        Self {
            id,
            map: Arc::new(SkipMap::new()),
            wal: None,
            approximate_size: Arc::new(AtomicUsize::new(0)),
        }
    }

    /// Create a new mem-table with WAL
    pub fn create_with_wal(id: usize, path: impl AsRef<Path>) -> Result<Self> {
        Ok(Self {
            id,
            map: Arc::new(SkipMap::new()),
            wal: Some(Wal::create(path.as_ref())?),
            approximate_size: Arc::new(AtomicUsize::new(0)),
        })
    }

    /// Create a memtable from WAL
    pub fn recover_from_wal(id: usize, path: impl AsRef<Path>) -> Result<Self> {
        let map = Arc::new(SkipMap::new());
        Ok(Self {
            id,
            wal: Some(Wal::recover(path.as_ref(), &map)?),
            map,
            approximate_size: Arc::new(AtomicUsize::new(0)),
        })
    }

    /// Get a value by key.
    pub fn get(&self, key: KeySlice) -> Option<Bytes> {
        let key_bytes = KeyBytes::from_bytes_with_ts(
            Bytes::from_static(unsafe { std::mem::transmute(key.key_ref()) }),
            key.ts(),
        );

        self.map.get(&key_bytes).map(|e| e.value().clone())
    }

    /// Put a key-value pair into the mem-table.
    pub fn put(&self, key: KeySlice, value: &[u8]) -> Result<()> {
        let estimated_size = key.raw_len() + value.len();
        self.map.insert(
            key.to_key_vec().into_key_bytes(),
            Bytes::copy_from_slice(value),
        );
        self.approximate_size
            .fetch_add(estimated_size, std::sync::atomic::Ordering::Relaxed);
        if let Some(ref wal) = self.wal {
            wal.put(key, value)?;
        }
        Ok(())
    }

    pub fn sync_wal(&self) -> Result<()> {
        if let Some(ref wal) = self.wal {
            wal.sync()?;
        }
        Ok(())
    }

    /// Get an iterator over a range of keys.
    pub fn scan(&self, lower: Bound<KeySlice>, upper: Bound<KeySlice>) -> MemTableIterator {
        let (lower, upper) = (map_key_bound(lower), map_key_bound(upper));
        let mut iter = MemTableIteratorBuilder {
            map: self.map.clone(),
            iter_builder: |map| map.range((lower, upper)),
            item: (KeyBytes::new(), Bytes::new()),
        }
        .build();
        let entry = iter.with_iter_mut(|iter| MemTableIterator::entry_to_item(iter.next()));
        iter.with_mut(|x| *x.item = entry);
        iter
    }

    pub fn flush(&self, builder: &mut SsTableBuilder) -> Result<()> {
        for entry in self.map.iter() {
            builder.add(entry.key().as_key_slice(), &entry.value()[..]);
        }
        Ok(())
    }

    pub fn id(&self) -> usize {
        self.id
    }

    pub fn approximate_size(&self) -> usize {
        self.approximate_size
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Only use this function when closing the database
    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }
    pub fn for_testing_put_slice(&self, key: &[u8], value: &[u8]) -> Result<()> {
        self.put(KeySlice::from_slice(key, TS_DEFAULT), value)
    }

    pub fn for_testing_get_slice(&self, key: &[u8]) -> Option<Bytes> {
        self.get(KeySlice::from_slice(key, TS_DEFAULT))
    }

    pub fn for_testing_scan_slice(
        &self,
        lower: Bound<&[u8]>,
        upper: Bound<&[u8]>,
    ) -> MemTableIterator {
        self.scan(
            map_key_bound_plus_ts(lower, TS_DEFAULT),
            map_key_bound_plus_ts(upper, TS_DEFAULT),
        )
    }
}

type SkipMapRangeIter<'a> = crossbeam_skiplist::map::Range<
    'a,
    KeyBytes,
    (Bound<KeyBytes>, Bound<KeyBytes>),
    KeyBytes,
    Bytes,
>;

#[self_referencing]
pub struct MemTableIterator {
    /// Stores a reference to the skipmap.
    map: Arc<SkipMap<KeyBytes, Bytes>>,
    /// Stores a skipmap iterator that refers to the lifetime of `MemTableIterator` itself.
    #[borrows(map)]
    #[not_covariant]
    iter: SkipMapRangeIter<'this>,
    /// Stores the current key-value pair.
    item: (KeyBytes, Bytes),
}

impl MemTableIterator {
    fn entry_to_item(entry: Option<Entry<'_, KeyBytes, Bytes>>) -> (KeyBytes, Bytes) {
        entry
            .map(|x| (x.key().clone(), x.value().clone()))
            .unwrap_or_else(|| (KeyBytes::new(), Bytes::new()))
    }
}

impl StorageIterator for MemTableIterator {
    type KeyType<'a> = KeySlice<'a>;

    fn value(&self) -> &[u8] {
        &self.borrow_item().1[..]
    }

    fn key(&self) -> KeySlice {
        self.borrow_item().0.as_key_slice()
    }

    fn is_valid(&self) -> bool {
        !self.borrow_item().0.is_empty()
    }

    fn _next(&mut self) -> Result<()> {
        let entry = self.with_iter_mut(|iter| MemTableIterator::entry_to_item(iter.next()));
        self.with_mut(|x| *x.item = entry);
        Ok(())
    }
}
#[cfg(test)]
mod test {
    use std::{ops::Bound, sync::Arc};
    use bytes::Bytes;
    use tempfile::tempdir;

    use crate::storage::engine::piggykv::{
        debug::{check_iter_result_by_key, check_lsm_iter_result_by_key, expect_iter_error, MockIterator}, iterators::{merge_iterator::MergeIterator, StorageIterator}, lsm_iterator::FusedIterator, lsm_storage::{LsmStorageInner, LsmStorageOptions}, mem_table::MemTable
    };

    #[test]
    fn test_task1_memtable_get() {
        let memtable = MemTable::create(0);
        memtable.for_testing_put_slice(b"key1", b"value1").unwrap();
        memtable.for_testing_put_slice(b"key2", b"value2").unwrap();
        memtable.for_testing_put_slice(b"key3", b"value3").unwrap();
        assert_eq!(
            &memtable.for_testing_get_slice(b"key1").unwrap()[..],
            b"value1"
        );
        assert_eq!(
            &memtable.for_testing_get_slice(b"key2").unwrap()[..],
            b"value2"
        );
        assert_eq!(
            &memtable.for_testing_get_slice(b"key3").unwrap()[..],
            b"value3"
        );
    }

    #[test]
    fn test_task1_memtable_overwrite() {
        let memtable = MemTable::create(0);
        memtable.for_testing_put_slice(b"key1", b"value1").unwrap();
        memtable.for_testing_put_slice(b"key2", b"value2").unwrap();
        memtable.for_testing_put_slice(b"key3", b"value3").unwrap();
        memtable.for_testing_put_slice(b"key1", b"value11").unwrap();
        memtable.for_testing_put_slice(b"key2", b"value22").unwrap();
        memtable.for_testing_put_slice(b"key3", b"value33").unwrap();
        assert_eq!(
            &memtable.for_testing_get_slice(b"key1").unwrap()[..],
            b"value11"
        );
        assert_eq!(
            &memtable.for_testing_get_slice(b"key2").unwrap()[..],
            b"value22"
        );
        assert_eq!(
            &memtable.for_testing_get_slice(b"key3").unwrap()[..],
            b"value33"
        );
    }

    #[test]
    fn test_task2_storage_integration() {
        let dir = tempdir().unwrap();
        let storage =
            Arc::new(LsmStorageInner::open(dir.path(), LsmStorageOptions::default()).unwrap());
        assert_eq!(&storage.get(b"0").unwrap(), &None);
        storage.put(b"1", b"233").unwrap();
        storage.put(b"2", b"2333").unwrap();
        storage.put(b"3", b"23333").unwrap();
        assert_eq!(&storage.get(b"1").unwrap().unwrap()[..], b"233");
        assert_eq!(&storage.get(b"2").unwrap().unwrap()[..], b"2333");
        assert_eq!(&storage.get(b"3").unwrap().unwrap()[..], b"23333");
        storage.delete(b"2").unwrap();
        assert!(storage.get(b"2").unwrap().is_none());
        storage.delete(b"0").unwrap(); // should NOT report any error
    }

    #[test]
    fn test_task3_storage_integration() {
        let dir = tempdir().unwrap();
        let storage =
            Arc::new(LsmStorageInner::open(dir.path(), LsmStorageOptions::default()).unwrap());
        storage.put(b"1", b"233").unwrap();
        storage.put(b"2", b"2333").unwrap();
        storage.put(b"3", b"23333").unwrap();
        storage
            .force_freeze_memtable(&storage.state_lock.lock())
            .unwrap();
        assert_eq!(storage.state.read().imm_memtables.len(), 1);
        let previous_approximate_size = storage.state.read().imm_memtables[0].approximate_size();
        assert!(previous_approximate_size >= 15);
        storage.put(b"1", b"2333").unwrap();
        storage.put(b"2", b"23333").unwrap();
        storage.put(b"3", b"233333").unwrap();
        storage
            .force_freeze_memtable(&storage.state_lock.lock())
            .unwrap();
        assert_eq!(storage.state.read().imm_memtables.len(), 2);
        assert!(
            storage.state.read().imm_memtables[1].approximate_size() == previous_approximate_size,
            "wrong order of memtables?"
        );
        assert!(
            storage.state.read().imm_memtables[0].approximate_size() > previous_approximate_size
        );
    }

    #[test]
    fn test_task3_freeze_on_capacity() {
        let dir = tempdir().unwrap();
        let mut options = LsmStorageOptions::default();
        options.target_sst_size = 1024;
        options.num_memtable_limit = 1000;
        let storage = Arc::new(LsmStorageInner::open(dir.path(), options).unwrap());
        for _ in 0..1000 {
            storage.put(b"1", b"2333").unwrap();
        }
        let num_imm_memtables = storage.state.read().imm_memtables.len();
        assert!(num_imm_memtables >= 1, "no memtable frozen?");
        for _ in 0..1000 {
            storage.delete(b"1").unwrap();
        }
        assert!(
            storage.state.read().imm_memtables.len() > num_imm_memtables,
            "no more memtable frozen?"
        );
    }

    #[test]
    fn test_task4_storage_integration() {
        let dir = tempdir().unwrap();
        let storage =
            Arc::new(LsmStorageInner::open(dir.path(), LsmStorageOptions::default()).unwrap());
        assert_eq!(&storage.get(b"0").unwrap(), &None);
        storage.put(b"1", b"233").unwrap();
        storage.put(b"2", b"2333").unwrap();
        storage.put(b"3", b"23333").unwrap();
        storage
            .force_freeze_memtable(&storage.state_lock.lock())
            .unwrap();
        storage.delete(b"1").unwrap();
        storage.delete(b"2").unwrap();
        storage.put(b"3", b"2333").unwrap();
        storage.put(b"4", b"23333").unwrap();
        storage
            .force_freeze_memtable(&storage.state_lock.lock())
            .unwrap();
        storage.put(b"1", b"233333").unwrap();
        storage.put(b"3", b"233333").unwrap();
        assert_eq!(storage.state.read().imm_memtables.len(), 2);
        assert_eq!(&storage.get(b"1").unwrap().unwrap()[..], b"233333");
        assert_eq!(&storage.get(b"2").unwrap(), &None);
        assert_eq!(&storage.get(b"3").unwrap().unwrap()[..], b"233333");
        assert_eq!(&storage.get(b"4").unwrap().unwrap()[..], b"23333");
    }
    #[test]
    fn test_task1_memtable_iter() {
        use std::ops::Bound;
        let memtable = MemTable::create(0);
        memtable.for_testing_put_slice(b"key1", b"value1").unwrap();
        memtable.for_testing_put_slice(b"key2", b"value2").unwrap();
        memtable.for_testing_put_slice(b"key3", b"value3").unwrap();

        {
            let mut iter = memtable.for_testing_scan_slice(Bound::Unbounded, Bound::Unbounded);
            assert_eq!(iter.key().for_testing_key_ref(), b"key1");
            assert_eq!(iter.value(), b"value1");
            assert!(iter.is_valid());
            iter._next().unwrap();
            assert_eq!(iter.key().for_testing_key_ref(), b"key2");
            assert_eq!(iter.value(), b"value2");
            assert!(iter.is_valid());
            iter._next().unwrap();
            assert_eq!(iter.key().for_testing_key_ref(), b"key3");
            assert_eq!(iter.value(), b"value3");
            assert!(iter.is_valid());
            iter._next().unwrap();
            assert!(!iter.is_valid());
        }

        {
            let mut iter =
                memtable.for_testing_scan_slice(Bound::Included(b"key1"), Bound::Included(b"key2"));
            assert_eq!(iter.key().for_testing_key_ref(), b"key1");
            assert_eq!(iter.value(), b"value1");
            assert!(iter.is_valid());
            iter._next().unwrap();
            assert_eq!(iter.key().for_testing_key_ref(), b"key2");
            assert_eq!(iter.value(), b"value2");
            assert!(iter.is_valid());
            iter._next().unwrap();
            assert!(!iter.is_valid());
        }

        {
            let mut iter =
                memtable.for_testing_scan_slice(Bound::Excluded(b"key1"), Bound::Excluded(b"key3"));
            assert_eq!(iter.key().for_testing_key_ref(), b"key2");
            assert_eq!(iter.value(), b"value2");
            assert!(iter.is_valid());
            iter._next().unwrap();
            assert!(!iter.is_valid());
        }
    }

    #[test]
    fn test_task1_empty_memtable_iter() {
        use std::ops::Bound;
        let memtable = MemTable::create(0);
        {
            let iter =
                memtable.for_testing_scan_slice(Bound::Excluded(b"key1"), Bound::Excluded(b"key3"));
            assert!(!iter.is_valid());
        }
        {
            let iter =
                memtable.for_testing_scan_slice(Bound::Included(b"key1"), Bound::Included(b"key2"));
            assert!(!iter.is_valid());
        }
        {
            let iter = memtable.for_testing_scan_slice(Bound::Unbounded, Bound::Unbounded);
            assert!(!iter.is_valid());
        }
    }

    #[test]
    fn test_task2_merge_1() {
        let i1 = MockIterator::new(vec![
            (Bytes::from("a"), Bytes::from("1.1")),
            (Bytes::from("b"), Bytes::from("2.1")),
            (Bytes::from("c"), Bytes::from("3.1")),
            (Bytes::from("e"), Bytes::new()),
        ]);
        let i2 = MockIterator::new(vec![
            (Bytes::from("a"), Bytes::from("1.2")),
            (Bytes::from("b"), Bytes::from("2.2")),
            (Bytes::from("c"), Bytes::from("3.2")),
            (Bytes::from("d"), Bytes::from("4.2")),
        ]);
        let i3 = MockIterator::new(vec![
            (Bytes::from("b"), Bytes::from("2.3")),
            (Bytes::from("c"), Bytes::from("3.3")),
            (Bytes::from("d"), Bytes::from("4.3")),
        ]);

        let mut iter = MergeIterator::create(vec![
            Box::new(i1.clone()),
            Box::new(i2.clone()),
            Box::new(i3.clone()),
        ]);

        check_iter_result_by_key(
            &mut iter,
            vec![
                (Bytes::from("a"), Bytes::from("1.1")),
                (Bytes::from("b"), Bytes::from("2.1")),
                (Bytes::from("c"), Bytes::from("3.1")),
                (Bytes::from("d"), Bytes::from("4.2")),
                (Bytes::from("e"), Bytes::new()),
            ],
        );

        let mut iter = MergeIterator::create(vec![Box::new(i3), Box::new(i1), Box::new(i2)]);

        check_iter_result_by_key(
            &mut iter,
            vec![
                (Bytes::from("a"), Bytes::from("1.1")),
                (Bytes::from("b"), Bytes::from("2.3")),
                (Bytes::from("c"), Bytes::from("3.3")),
                (Bytes::from("d"), Bytes::from("4.3")),
                (Bytes::from("e"), Bytes::new()),
            ],
        );
    }

    #[test]
    fn test_task2_merge_2() {
        let i1 = MockIterator::new(vec![
            (Bytes::from("a"), Bytes::from("1.1")),
            (Bytes::from("b"), Bytes::from("2.1")),
            (Bytes::from("c"), Bytes::from("3.1")),
        ]);
        let i2 = MockIterator::new(vec![
            (Bytes::from("d"), Bytes::from("1.2")),
            (Bytes::from("e"), Bytes::from("2.2")),
            (Bytes::from("f"), Bytes::from("3.2")),
            (Bytes::from("g"), Bytes::from("4.2")),
        ]);
        let i3 = MockIterator::new(vec![
            (Bytes::from("h"), Bytes::from("1.3")),
            (Bytes::from("i"), Bytes::from("2.3")),
            (Bytes::from("j"), Bytes::from("3.3")),
            (Bytes::from("k"), Bytes::from("4.3")),
        ]);
        let i4 = MockIterator::new(vec![]);
        let result = vec![
            (Bytes::from("a"), Bytes::from("1.1")),
            (Bytes::from("b"), Bytes::from("2.1")),
            (Bytes::from("c"), Bytes::from("3.1")),
            (Bytes::from("d"), Bytes::from("1.2")),
            (Bytes::from("e"), Bytes::from("2.2")),
            (Bytes::from("f"), Bytes::from("3.2")),
            (Bytes::from("g"), Bytes::from("4.2")),
            (Bytes::from("h"), Bytes::from("1.3")),
            (Bytes::from("i"), Bytes::from("2.3")),
            (Bytes::from("j"), Bytes::from("3.3")),
            (Bytes::from("k"), Bytes::from("4.3")),
        ];

        let mut iter = MergeIterator::create(vec![
            Box::new(i1.clone()),
            Box::new(i2.clone()),
            Box::new(i3.clone()),
            Box::new(i4.clone()),
        ]);
        check_iter_result_by_key(&mut iter, result.clone());

        let mut iter = MergeIterator::create(vec![
            Box::new(i2.clone()),
            Box::new(i4.clone()),
            Box::new(i3.clone()),
            Box::new(i1.clone()),
        ]);
        check_iter_result_by_key(&mut iter, result.clone());

        let mut iter =
            MergeIterator::create(vec![Box::new(i4), Box::new(i3), Box::new(i2), Box::new(i1)]);
        check_iter_result_by_key(&mut iter, result);
    }

    #[test]
    fn test_task2_merge_empty() {
        let mut iter = MergeIterator::<MockIterator>::create(vec![]);
        check_iter_result_by_key(&mut iter, vec![]);

        let i1 = MockIterator::new(vec![
            (Bytes::from("a"), Bytes::from("1.1")),
            (Bytes::from("b"), Bytes::from("2.1")),
            (Bytes::from("c"), Bytes::from("3.1")),
        ]);
        let i2 = MockIterator::new(vec![]);
        let mut iter = MergeIterator::<MockIterator>::create(vec![Box::new(i1), Box::new(i2)]);
        check_iter_result_by_key(
            &mut iter,
            vec![
                (Bytes::from("a"), Bytes::from("1.1")),
                (Bytes::from("b"), Bytes::from("2.1")),
                (Bytes::from("c"), Bytes::from("3.1")),
            ],
        );
    }

    #[test]
    fn test_task2_merge_error() {
        let mut iter = MergeIterator::<MockIterator>::create(vec![]);
        check_iter_result_by_key(&mut iter, vec![]);

        let i1 = MockIterator::new(vec![
            (Bytes::from("a"), Bytes::from("1.1")),
            (Bytes::from("b"), Bytes::from("2.1")),
            (Bytes::from("c"), Bytes::from("3.1")),
        ]);
        let i2 = MockIterator::new_with_error(
            vec![
                (Bytes::from("a"), Bytes::from("1.1")),
                (Bytes::from("b"), Bytes::from("2.1")),
                (Bytes::from("c"), Bytes::from("3.1")),
            ],
            1,
        );
        let iter = MergeIterator::<MockIterator>::create(vec![
            Box::new(i1.clone()),
            Box::new(i1),
            Box::new(i2),
        ]);
        // your implementation should correctly throw an error instead of panic
        expect_iter_error(iter);
    }

    #[test]
    fn test_task3_fused_iterator() {
        let iter = MockIterator::new(vec![]);
        let mut fused_iter = FusedIterator::new(iter);
        assert!(!fused_iter.is_valid());
        fused_iter._next().unwrap();
        fused_iter._next().unwrap();
        fused_iter._next().unwrap();
        assert!(!fused_iter.is_valid());

        let iter = MockIterator::new_with_error(
            vec![
                (Bytes::from("a"), Bytes::from("1.1")),
                (Bytes::from("a"), Bytes::from("1.1")),
            ],
            1,
        );
        let mut fused_iter = FusedIterator::new(iter);
        assert!(fused_iter.is_valid());
        assert!(fused_iter._next().is_err());
        assert!(!fused_iter.is_valid());
        assert!(fused_iter._next().is_err());
        assert!(fused_iter._next().is_err());
    }

    #[test]
    fn test_task4_integration() {
        let dir = tempdir().unwrap();
        let storage =
            Arc::new(LsmStorageInner::open(dir.path(), LsmStorageOptions::default()).unwrap());
        storage.put(b"1", b"233").unwrap();
        storage.put(b"2", b"2333").unwrap();
        storage.put(b"3", b"23333").unwrap();
        storage
            .force_freeze_memtable(&storage.state_lock.lock())
            .unwrap();
        storage.delete(b"1").unwrap();
        storage.delete(b"2").unwrap();
        storage.put(b"3", b"2333").unwrap();
        storage.put(b"4", b"23333").unwrap();
        storage
            .force_freeze_memtable(&storage.state_lock.lock())
            .unwrap();
        storage.put(b"1", b"233333").unwrap();
        storage.put(b"3", b"233333").unwrap();
        {
            let mut iter = storage.scan(Bound::Unbounded, Bound::Unbounded).unwrap();
            check_lsm_iter_result_by_key(
                &mut iter,
                vec![
                    (Bytes::from_static(b"1"), Bytes::from_static(b"233333")),
                    (Bytes::from_static(b"3"), Bytes::from_static(b"233333")),
                    (Bytes::from_static(b"4"), Bytes::from_static(b"23333")),
                ],
            );
            assert!(!iter.is_valid());
            iter._next().unwrap();
            iter._next().unwrap();
            iter._next().unwrap();
            assert!(!iter.is_valid());
        }
        {
            let mut iter = storage
                .scan(Bound::Included(b"2"), Bound::Included(b"3"))
                .unwrap();
            check_lsm_iter_result_by_key(
                &mut iter,
                vec![(Bytes::from_static(b"3"), Bytes::from_static(b"233333"))],
            );
            assert!(!iter.is_valid());
            iter._next().unwrap();
            iter._next().unwrap();
            iter._next().unwrap();
            assert!(!iter.is_valid());
        }
    }
}
