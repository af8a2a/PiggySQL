use std::{
    fmt::Display,
    io::Read,
    path::{Path, PathBuf},
    sync::Arc,
};
pub mod block;
pub mod compact;
pub mod iterators;
pub mod key;
pub mod lsm_iterator;
pub mod lsm_storage;
pub mod manifest;
pub mod memtable;
pub mod table;
pub mod wal;

use self::{
    iterators::StorageIterator,
    lsm_storage::{LsmStorageOptions, MiniLsm},
};

use super::Result;
use super::StorageEngine;

pub struct LSM {
    pub inner: Arc<MiniLsm>,
}

impl LSM {
    pub fn new(path: impl AsRef<Path>, option: LsmStorageOptions) -> Self {
        Self {
            inner: MiniLsm::open(path, option).unwrap(),
        }
    }
}
impl Display for LSM {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "lsm")
    }
}

impl StorageEngine for LSM {
    fn delete(&self, key: &[u8]) -> super::Result<()> {
        self.inner.delete(key).unwrap();
        Ok(())
    }

    fn flush(&self) -> super::Result<()> {
        self.inner.force_flush()?;
        self.inner.sync().unwrap();
        Ok(())
    }

    fn get(&self, key: &[u8]) -> super::Result<Option<Vec<u8>>> {
        let val = self.inner.get(key).unwrap();
        Ok(val.map(|v| v.to_vec()))
    }

    fn scan(&self, range: impl std::ops::RangeBounds<Vec<u8>>) -> super::Result<super::KvScan> {
        let start = match range.start_bound() {
            std::ops::Bound::Included(v) => std::ops::Bound::Included(v.as_slice()),
            std::ops::Bound::Excluded(v) => std::ops::Bound::Excluded(v.as_slice()),
            std::ops::Bound::Unbounded => std::ops::Bound::Unbounded,
        };
        let end = match range.end_bound() {
            std::ops::Bound::Included(v) => std::ops::Bound::Included(v.as_slice()),
            std::ops::Bound::Excluded(v) => std::ops::Bound::Excluded(v.as_slice()),
            std::ops::Bound::Unbounded => std::ops::Bound::Unbounded,
        };
        let mut iter = self.inner.scan(start, end).unwrap();
        let mut kv = vec![];
        while iter.is_valid() {
            kv.push((iter.key().to_vec(), iter.value().to_vec()));
            iter.next().ok().unwrap();
        }
        Ok(Box::new(kv.into_iter().map(Ok)))
    }

    fn set(&self, key: &[u8], value: Vec<u8>) -> super::Result<()> {
        self.inner.put(key, &value).unwrap();
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::errors::Result;
    use crate::storage::engine::tests::test_engine;
    test_engine!({
        let path = tempdir::TempDir::new("piggydb")
            .unwrap()
            .path()
            .join("piggydb");
        LSM::new(path, LsmStorageOptions::default())
    });
}
