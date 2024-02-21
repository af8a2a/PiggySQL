use crate::errors::Result;

use super::iterators::{MergeIter, StorageIter, TwoMergeIter};
use super::lsm_storage::DiskIter;
use super::memtable::MemTableIter;

type LsmIterInner = TwoMergeIter<MergeIter<MemTableIter>, MergeIter<DiskIter>>;

#[derive(Clone)]
pub struct LsmIter {
    inner_iter: LsmIterInner,
}

impl LsmIter {
    pub fn create(inner_iter: LsmIterInner) -> Self {
        Self { inner_iter }
    }

    fn try_next(&mut self) -> Result<Option<(Vec<u8>, Vec<u8>)>> {
        while let Some((key, value)) = self.inner_iter.try_next()? {
            if !value.is_empty() {
                return Ok(Some((key, value)));
            }
        }
        Ok(None)
    }

    fn try_next_back(&mut self) -> Result<Option<(Vec<u8>, Vec<u8>)>> {
        while let Some((key, value)) = self.inner_iter.try_next_back()? {
            if !value.is_empty() {
                return Ok(Some((key, value)));
            }
        }
        Ok(None)
    }
}

impl Iterator for LsmIter {
    type Item = Result<(Vec<u8>, Vec<u8>)>;

    fn next(&mut self) -> Option<Self::Item> {
        self.try_next().transpose()
    }
}

impl DoubleEndedIterator for LsmIter {
    fn next_back(&mut self) -> Option<Self::Item> {
        self.try_next_back().transpose()
    }
}

