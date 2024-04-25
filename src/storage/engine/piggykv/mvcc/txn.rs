#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use std::{
    collections::HashSet,
    ops::Bound,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};

use bytes::Bytes;
use crossbeam_skiplist::{map::Entry, SkipMap};
use itertools::Itertools;
use ouroboros::self_referencing;
use parking_lot::Mutex;
use tracing::error;

use super::CommittedTxnData;
use crate::errors::{DatabaseError, Result};
use crate::storage::engine::piggykv::{
    iterators::{two_merge_iterator::TwoMergeIterator, StorageIterator},
    lsm_iterator::{FusedIterator, LsmIterator},
    lsm_storage::{LsmStorageInner, WriteBatchRecord},
    mem_table::map_bound,
};

pub struct Transaction {
    pub(crate) read_ts: u64,
    pub(crate) inner: Arc<LsmStorageInner>,
    pub(crate) local_storage: Arc<SkipMap<Bytes, Bytes>>,
    pub(crate) committed: Arc<AtomicBool>,
    /// Write set and read set
    pub(crate) key_hashes: Option<Mutex<(HashSet<u32>, HashSet<u32>)>>,
}

impl Transaction {
    pub fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        if self.committed.load(Ordering::SeqCst) {
            error!("cannot operate on committed txn!");
            return Err(DatabaseError::InternalError(
                "cannot operate on committed txn!".to_string(),
            ));
        }
        if let Some(guard) = &self.key_hashes {
            let mut guard = guard.lock();
            let (_, read_set) = &mut *guard;
            read_set.insert(farmhash::hash32(key));
        }
        if let Some(entry) = self.local_storage.get(key) {
            if entry.value().is_empty() {
                return Ok(None);
            } else {
                return Ok(Some(entry.value().clone()));
            }
        }
        self.inner.get_with_ts(key, self.read_ts)
    }

    pub fn scan(self: &Arc<Self>, lower: Bound<&[u8]>, upper: Bound<&[u8]>) -> Result<TxnIterator> {
        if self.committed.load(Ordering::SeqCst) {
            panic!("cannot operate on committed txn!");
        }
        let mut local_iter = TxnLocalIteratorBuilder {
            map: self.local_storage.clone(),
            iter_builder: |map| map.range((map_bound(lower), map_bound(upper))),
            item: (Bytes::new(), Bytes::new()),
        }
        .build();
        let entry = local_iter.with_iter_mut(|iter| TxnLocalIterator::entry_to_item(iter.next()));
        local_iter.with_mut(|x| *x.item = entry);
        let inner_scan = self.inner.scan_with_ts(lower, upper, self.read_ts)?;

        let iter = TwoMergeIterator::create(local_iter, inner_scan)?;
        TxnIterator::create(self.clone(), iter)
    }

    pub fn put(&self, key: &[u8], value: &[u8]) {
        if self.committed.load(Ordering::SeqCst) {
            panic!("cannot operate on committed txn!");
        }
        self.local_storage
            .insert(Bytes::copy_from_slice(key), Bytes::copy_from_slice(value));
        if let Some(key_hashes) = &self.key_hashes {
            let mut key_hashes = key_hashes.lock();
            let (write_hashes, _) = &mut *key_hashes;
            write_hashes.insert(farmhash::hash32(key));
        }
    }

    pub fn delete(&self, key: &[u8]) {
        if self.committed.load(Ordering::SeqCst) {
            panic!("cannot operate on committed txn!");
        }
        self.local_storage
            .insert(Bytes::copy_from_slice(key), Bytes::new());
        if let Some(key_hashes) = &self.key_hashes {
            let mut key_hashes = key_hashes.lock();
            let (write_hashes, _) = &mut *key_hashes;
            write_hashes.insert(farmhash::hash32(key));
        }
    }

    pub fn commit(&self) -> Result<()> {
        self.committed
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .expect("cannot operate on committed txn!");
        let _commit_lock = self.inner.mvcc().commit_lock.lock();
        let serializability_check;
        if let Some(guard) = &self.key_hashes {
            let guard = guard.lock();
            let (write_set, read_set) = &*guard;
            // println!(
            //     "commit txn {}: write_set: {:?}, read_set: {:?}",
            //     self.read_ts,write_set, read_set
            // );
            if !write_set.is_empty() {
                let committed_txns = self.inner.mvcc().committed_txns.lock();
                for (_, txn_data) in committed_txns.range((self.read_ts + 1)..) {

                    for key_hash in read_set {
                        if txn_data.key_hashes.contains(key_hash) {
                            return Err(DatabaseError::Serialization);
                        }
                    }
                }
            }
            serializability_check = true;
        } else {
            serializability_check = false;
        }
        let batch = self
            .local_storage
            .iter()
            .map(|entry| {
                if entry.value().is_empty() {
                    WriteBatchRecord::Del(entry.key().clone())
                } else {
                    WriteBatchRecord::Put(entry.key().clone(), entry.value().clone())
                }
            })
            .collect::<Vec<_>>();
        let ts = self.inner.write_batch_inner(&batch)?;
        if serializability_check {
            let mut committed_txns = self.inner.mvcc().committed_txns.lock();
            let mut key_hashes = self.key_hashes.as_ref().unwrap().lock();
            let (write_set, _) = &mut *key_hashes;

            let old_data = committed_txns.insert(
                ts,
                CommittedTxnData {
                    key_hashes: std::mem::take(write_set),
                    read_ts: self.read_ts,
                    commit_ts: ts,
                },
            );
            assert!(old_data.is_none());

            // remove unneeded txn data
            let watermark = self.inner.mvcc().watermark();
            while let Some(entry) = committed_txns.first_entry() {
                if *entry.key() < watermark {
                    entry.remove();
                } else {
                    break;
                }
            }
        }

        Ok(())
    }
    pub fn debug(&self) {
        let snapshot = {
            let guard = self.inner.state.write();
            guard
        };
        for sst in snapshot.l0_sstables.iter() {
            println!(
                "l0 sst: {:?},first_key:{:?},last_key:{:?}",
                sst,
                snapshot.sstables[sst].first_key(),
                snapshot.sstables[sst].last_key()
            );
        }
        for level in snapshot.levels.iter() {
            for sst in level.1.iter() {
                println!(
                    "l{} sst: {:?},first_key:{:?},last_key:{:?}",
                    level.0,
                    sst,
                    snapshot.sstables[sst].first_key(),
                    snapshot.sstables[sst].last_key()
                );
            }
        }
    }
}

impl Drop for Transaction {
    fn drop(&mut self) {
        self.inner.mvcc().ts.lock().1.remove_reader(self.read_ts)
    }
}

type SkipMapRangeIter<'a> =
    crossbeam_skiplist::map::Range<'a, Bytes, (Bound<Bytes>, Bound<Bytes>), Bytes, Bytes>;

impl TxnLocalIterator {
    fn entry_to_item(entry: Option<Entry<'_, Bytes, Bytes>>) -> (Bytes, Bytes) {
        entry
            .map(|x| (x.key().clone(), x.value().clone()))
            .unwrap_or_else(|| (Bytes::new(), Bytes::new()))
    }
}
#[self_referencing]
pub struct TxnLocalIterator {
    /// Stores a reference to the skipmap.
    map: Arc<SkipMap<Bytes, Bytes>>,
    /// Stores a skipmap iterator that refers to the lifetime of `MemTableIterator` itself.
    #[borrows(map)]
    #[not_covariant]
    iter: SkipMapRangeIter<'this>,
    /// Stores the current key-value pair.
    item: (Bytes, Bytes),
}

impl StorageIterator for TxnLocalIterator {
    type KeyType<'a> = &'a [u8];

    fn value(&self) -> &[u8] {
        &self.borrow_item().1[..]
    }

    fn key(&self) -> &[u8] {
        &self.borrow_item().0[..]
    }

    fn is_valid(&self) -> bool {
        !self.borrow_item().0.is_empty()
    }

    fn _next(&mut self) -> Result<()> {
        let entry = self.with_iter_mut(|iter| TxnLocalIterator::entry_to_item(iter.next()));
        self.with_mut(|x| *x.item = entry);
        Ok(())
    }
}

pub struct TxnIterator {
    txn: Arc<Transaction>,
    iter: TwoMergeIterator<TxnLocalIterator, FusedIterator<LsmIterator>>,
}

impl TxnIterator {
    pub fn create(
        txn: Arc<Transaction>,
        iter: TwoMergeIterator<TxnLocalIterator, FusedIterator<LsmIterator>>,
    ) -> Result<Self> {
        let mut iter = Self { txn, iter };

        iter.skip_deletes()?;
        if iter.is_valid() {
            iter.add_to_read_set(iter.key());
        }
        Ok(iter)
    }

    fn skip_deletes(&mut self) -> Result<()> {
        while self.iter.is_valid() && self.iter.value().is_empty() {
            println!("skip key={}", String::from_utf8(self.iter.key().to_vec())?);
            if !self.iter.value().is_empty() {
                println!("val={}", String::from_utf8(self.iter.value().to_vec())?);
            }
            self.iter._next()?;
        }
        Ok(())
    }

    fn add_to_read_set(&self, key: &[u8]) {
        if let Some(guard) = &self.txn.key_hashes {
            let mut guard = guard.lock();
            let (_, read_set) = &mut *guard;
            read_set.insert(farmhash::hash32(key));
        }
    }
}

impl StorageIterator for TxnIterator {
    type KeyType<'a> = &'a [u8] where Self: 'a;

    fn value(&self) -> &[u8] {
        self.iter.value()
    }

    fn key(&self) -> Self::KeyType<'_> {
        self.iter.key()
    }

    fn is_valid(&self) -> bool {
        self.iter.is_valid()
    }

    fn _next(&mut self) -> Result<()> {
        self.iter._next()?;
        self.skip_deletes()?;
        if self.is_valid() {
            self.add_to_read_set(self.key());
        }

        Ok(())
    }

    fn num_active_iterators(&self) -> usize {
        self.iter.num_active_iterators()
    }
}

impl Iterator for TxnIterator {
    type Item = (Bytes, Bytes);

    fn next(&mut self) -> Option<Self::Item> {
        if self.is_valid() {
            let key = Bytes::copy_from_slice(self.key());
            let val = Bytes::copy_from_slice(self.value());
            self._next().ok();
            Some((key, val))
        } else {
            None
        }
    }
}

#[cfg(test)]
mod test {
    use std::ops::Bound;

    use bytes::Bytes;
    use tempfile::tempdir;

    use crate::storage::engine::piggykv::{
        compact::CompactionOptions, debug::check_lsm_iter_result_by_key,
        iterators::StorageIterator, lsm_storage::LsmStorageOptions, PiggyKV,
    };

    #[test]
    fn test_txn_integration() {
        let dir = tempdir().unwrap();
        let options = LsmStorageOptions::default_for_mvcc_test(CompactionOptions::NoCompaction);
        let storage = PiggyKV::open(&dir, options.clone()).unwrap();
        let txn1 = storage.new_txn().unwrap();
        let txn2 = storage.new_txn().unwrap();
        txn1.put(b"test1", b"233");
        txn2.put(b"test2", b"233");
        check_lsm_iter_result_by_key(
            &mut txn1.scan(Bound::Unbounded, Bound::Unbounded).unwrap(),
            vec![(Bytes::from("test1"), Bytes::from("233"))],
        );
        check_lsm_iter_result_by_key(
            &mut txn2.scan(Bound::Unbounded, Bound::Unbounded).unwrap(),
            vec![(Bytes::from("test2"), Bytes::from("233"))],
        );
        let txn3 = storage.new_txn().unwrap();
        check_lsm_iter_result_by_key(
            &mut txn3.scan(Bound::Unbounded, Bound::Unbounded).unwrap(),
            vec![],
        );
        txn1.commit().unwrap();
        txn2.commit().unwrap();
        check_lsm_iter_result_by_key(
            &mut txn3.scan(Bound::Unbounded, Bound::Unbounded).unwrap(),
            vec![],
        );
        drop(txn3);
        check_lsm_iter_result_by_key(
            &mut storage.scan(Bound::Unbounded, Bound::Unbounded).unwrap(),
            vec![
                (Bytes::from("test1"), Bytes::from("233")),
                (Bytes::from("test2"), Bytes::from("233")),
            ],
        );
        let txn4 = storage.new_txn().unwrap();
        assert_eq!(txn4.get(b"test1").unwrap(), Some(Bytes::from("233")));
        assert_eq!(txn4.get(b"test2").unwrap(), Some(Bytes::from("233")));
        check_lsm_iter_result_by_key(
            &mut txn4.scan(Bound::Unbounded, Bound::Unbounded).unwrap(),
            vec![
                (Bytes::from("test1"), Bytes::from("233")),
                (Bytes::from("test2"), Bytes::from("233")),
            ],
        );
        txn4.put(b"test2", b"2333");
        assert_eq!(txn4.get(b"test1").unwrap(), Some(Bytes::from("233")));
        assert_eq!(txn4.get(b"test2").unwrap(), Some(Bytes::from("2333")));
        check_lsm_iter_result_by_key(
            &mut txn4.scan(Bound::Unbounded, Bound::Unbounded).unwrap(),
            vec![
                (Bytes::from("test1"), Bytes::from("233")),
                (Bytes::from("test2"), Bytes::from("2333")),
            ],
        );
        txn4.delete(b"test2");
        assert_eq!(txn4.get(b"test1").unwrap(), Some(Bytes::from("233")));
        assert_eq!(txn4.get(b"test2").unwrap(), None);
        check_lsm_iter_result_by_key(
            &mut txn4.scan(Bound::Unbounded, Bound::Unbounded).unwrap(),
            vec![(Bytes::from("test1"), Bytes::from("233"))],
        );
    }
    #[test]
    fn test_serializable_1() {
        let dir = tempdir().unwrap();
        let mut options = LsmStorageOptions::default_for_mvcc_test(CompactionOptions::NoCompaction);
        options.serializable = true;
        let storage = PiggyKV::open(&dir, options.clone()).unwrap();
        storage.put(b"key1", b"1").unwrap();
        storage.put(b"key2", b"2").unwrap();
        let txn1 = storage.new_txn().unwrap();
        let txn2 = storage.new_txn().unwrap();
        txn1.put(b"key1", &txn1.get(b"key2").unwrap().unwrap());
        txn2.put(b"key2", &txn2.get(b"key1").unwrap().unwrap());
        txn1.commit().unwrap();
        assert!(txn2.commit().is_err());
        drop(txn2);
        assert_eq!(storage.get(b"key1").unwrap(), Some(Bytes::from("2")));
        assert_eq!(storage.get(b"key2").unwrap(), Some(Bytes::from("2")));
    }

    #[test]
    fn test_serializable_2() {
        let dir = tempdir().unwrap();
        let mut options = LsmStorageOptions::default_for_mvcc_test(CompactionOptions::NoCompaction);
        options.serializable = true;
        let storage = PiggyKV::open(&dir, options.clone()).unwrap();
        let txn1 = storage.new_txn().unwrap();
        let txn2 = storage.new_txn().unwrap();
        txn1.put(b"key1", b"1");
        txn2.put(b"key1", b"2");
        txn1.commit().unwrap();
        txn2.commit().unwrap();
        assert_eq!(storage.get(b"key1").unwrap(), Some(Bytes::from("2")));
    }

    #[test]
    fn test_serializable_3_ts_range() {
        let dir = tempdir().unwrap();
        let mut options = LsmStorageOptions::default_for_mvcc_test(CompactionOptions::NoCompaction);
        options.serializable = true;
        let storage = PiggyKV::open(&dir, options.clone()).unwrap();
        storage.put(b"key1", b"1").unwrap();
        storage.put(b"key2", b"2").unwrap();
        let txn1 = storage.new_txn().unwrap();
        txn1.put(b"key1", &txn1.get(b"key2").unwrap().unwrap());
        txn1.commit().unwrap();
        let txn2 = storage.new_txn().unwrap();
        txn2.put(b"key2", &txn2.get(b"key1").unwrap().unwrap());
        txn2.commit().unwrap();
        drop(txn2);
        assert_eq!(storage.get(b"key1").unwrap(), Some(Bytes::from("2")));
        assert_eq!(storage.get(b"key2").unwrap(), Some(Bytes::from("2")));
    }

    #[test]
    fn test_serializable_4_scan() {
        let dir = tempdir().unwrap();
        let mut options = LsmStorageOptions::default_for_mvcc_test(CompactionOptions::NoCompaction);
        options.serializable = true;
        let storage = PiggyKV::open(&dir, options.clone()).unwrap();
        storage.put(b"key1", b"1").unwrap();
        storage.put(b"key2", b"2").unwrap();
        let txn1 = storage.new_txn().unwrap();
        let txn2 = storage.new_txn().unwrap();
        txn1.put(b"key1", &txn1.get(b"key2").unwrap().unwrap());
        txn1.commit().unwrap();
        let iter = txn2.scan(Bound::Unbounded, Bound::Unbounded).unwrap();
        let consume = iter.count();

        assert_eq!(consume, 2);
        txn2.put(b"key2", b"1");
        assert!(txn2.commit().is_err());
        drop(txn2);
        assert_eq!(storage.get(b"key1").unwrap(), Some(Bytes::from("2")));
        assert_eq!(storage.get(b"key2").unwrap(), Some(Bytes::from("2")));
    }

    #[test]
    fn test_serializable_5_read_only() {
        let dir = tempdir().unwrap();
        let mut options = LsmStorageOptions::default_for_mvcc_test(CompactionOptions::NoCompaction);
        options.serializable = true;
        let storage = PiggyKV::open(&dir, options.clone()).unwrap();
        storage.put(b"key1", b"1").unwrap();
        storage.put(b"key2", b"2").unwrap();
        let txn1 = storage.new_txn().unwrap();
        txn1.put(b"key1", &txn1.get(b"key2").unwrap().unwrap());
        txn1.commit().unwrap();
        let txn2 = storage.new_txn().unwrap();
        txn2.get(b"key1").unwrap().unwrap();
        let iter = txn2.scan(Bound::Unbounded, Bound::Unbounded).unwrap();
        let _ = iter.count();
        txn2.commit().unwrap();
        assert_eq!(storage.get(b"key1").unwrap(), Some(Bytes::from("2")));
        assert_eq!(storage.get(b"key2").unwrap(), Some(Bytes::from("2")));
    }
}
