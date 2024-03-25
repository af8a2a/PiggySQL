use std::{ops::Bound, sync::Arc};

use crossbeam_skiplist::{map::Entry, SkipMap};
use ouroboros::self_referencing;

use super::{KvScan, StorageEngine};
use crate::errors::Result;
pub struct Memory {
    data: Arc<SkipMap<Vec<u8>, Vec<u8>>>,
}

impl Memory {
    /// Creates a new Memory key-value storage engine.
    pub fn new() -> Self {
        Self {
            data: Arc::new(SkipMap::new()),
        }
    }
}

impl std::fmt::Display for Memory {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "memory")
    }
}

impl StorageEngine for Memory {
    fn flush(&self) -> Result<()> {
        Ok(())
    }

    fn delete(&self, key: &[u8]) -> Result<()> {
        self.data.remove(key);
        Ok(())
    }

    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        Ok(self.data.get(key).map(|e| e.value().clone()))
    }

    fn scan(&self, range: impl std::ops::RangeBounds<Vec<u8>>) -> Result<KvScan> {
        let iter = ScanIteratorBuilder {
            map: self.data.clone(),
            iter_builder: |map| {
                map.range((range.start_bound().cloned(), range.end_bound().cloned()))
            },
            item: (Vec::new(), Vec::new()),
        }
        .build();
        Ok(Box::new(iter))
    }

    fn set(&self, key: &[u8], value: Vec<u8>) -> Result<()> {
        self.data.insert(key.to_vec(), value);
        Ok(())
    }
}
type SkipMapRangeIter<'a> =
    crossbeam_skiplist::map::Range<'a, Vec<u8>, (Bound<Vec<u8>>, Bound<Vec<u8>>), Vec<u8>, Vec<u8>>;
#[self_referencing]
pub struct ScanIterator {
    map: Arc<SkipMap<Vec<u8>, Vec<u8>>>,
    #[borrows(map)]
    #[not_covariant]
    iter: SkipMapRangeIter<'this>,
    item: (Vec<u8>, Vec<u8>),
}
impl ScanIterator {
    fn entry_to_item(entry: Option<Entry<'_, Vec<u8>, Vec<u8>>>) -> Option<(Vec<u8>, Vec<u8>)> {
        entry.map(|item| (item.key().clone(), item.value().clone()))
    }
}
impl Iterator for ScanIterator {
    type Item = Result<(Vec<u8>, Vec<u8>)>;

    fn next(&mut self) -> Option<Self::Item> {
        let entry = self.with_iter_mut(|iter| ScanIterator::entry_to_item(iter.next()));

        Ok(entry).transpose()
    }
}
impl DoubleEndedIterator for ScanIterator {
    fn next_back(&mut self) -> Option<Self::Item> {
        let entry = self.with_iter_mut(|iter| ScanIterator::entry_to_item(iter.next_back()));
        Ok(entry).transpose()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    super::super::tests::test_engine!(Arc::new(Memory::new()));
}
