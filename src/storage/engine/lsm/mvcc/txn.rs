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
use ouroboros::self_referencing;
use parking_lot::Mutex;

use super::CommittedTxnData;
use crate::errors::Result;
use crate::storage::engine::lsm::{
    iterators::{
        two_merge_iterator::{self, TwoMergeIterator},
        StorageIterator,
    },
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
        self.inner.get_with_ts(key, self.read_ts)
    }

    pub fn scan(self: &Arc<Self>, lower: Bound<&[u8]>, upper: Bound<&[u8]>) -> Result<TxnIterator> {
        TxnIterator::create(
            self.clone(),
            self.inner.scan_with_ts(lower, upper, self.read_ts)?,
        )

    }

    pub fn put(&self, key: &[u8], value: &[u8]) {
        unimplemented!()
    }

    pub fn delete(&self, key: &[u8]) {
        unimplemented!()
    }

    pub fn commit(&self) -> Result<()> {
        unimplemented!()
    }
}

impl Drop for Transaction {
    fn drop(&mut self) {}
}

type SkipMapRangeIter<'a> =
    crossbeam_skiplist::map::Range<'a, Bytes, (Bound<Bytes>, Bound<Bytes>), Bytes, Bytes>;

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
        unimplemented!()
    }

    fn key(&self) -> &[u8] {
        unimplemented!()
    }

    fn is_valid(&self) -> bool {
        unimplemented!()
    }

    fn next(&mut self) -> Result<()> {
        unimplemented!()
    }
}

pub struct TxnIterator {
    _txn: Arc<Transaction>,
    iter: FusedIterator<LsmIterator>,
}

impl TxnIterator {
    pub fn create(txn: Arc<Transaction>, iter: FusedIterator<LsmIterator>) -> Result<Self> {
        let iter = Self { _txn: txn, iter };
        Ok(iter)
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

    fn next(&mut self) -> Result<()> {
        self.iter.next()?;
        Ok(())

    }

    fn num_active_iterators(&self) -> usize {
        self.iter.num_active_iterators()
    }
}
