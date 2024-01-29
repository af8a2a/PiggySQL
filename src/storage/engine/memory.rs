use futures::future::ok;
use sled::transaction::ConflictableTransactionError;

use super::{StorageEngine, StorageEngineError};

/// An in-memory key/value storage engine using the Rust standard library B-tree.
pub struct Memory {
    store: sled::Db,
}
impl Drop for Memory {
    fn drop(&mut self) {
        self.store.flush().unwrap();
    }
}

impl Memory {
    /// Creates a new Memory key-value storage engine.
    pub fn new() -> Self {
        Self {
            store: sled::open("sled.db").expect("open"),
        }
    }
}

impl std::fmt::Display for Memory {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "memory")
    }
}

impl StorageEngine for Memory {
    type ScanIterator = ScanIterator;

    fn flush(&self) -> Result<(), StorageEngineError> {
        self.store.flush();
        Ok(())
    }

    fn delete(&self, key: &[u8]) -> Result<(), StorageEngineError> {
        let _ = self.store.remove(key).unwrap();
        Ok(())
    }

    fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        match self.store.get(key).unwrap() {
            Some(val) => Some(val.to_vec()),
            None => None,
        }
    }

    fn scan<R: std::ops::RangeBounds<Vec<u8>>>(&self, range: R) -> Self::ScanIterator {
        ScanIterator {
            inner: self.store.range(range),
        }
    }

    fn set(&self, key: &[u8], value: Vec<u8>) -> Result<(), StorageEngineError> {
        self.store.insert(key, value.to_vec());
        Ok(())
    }
}

pub struct ScanIterator {
    inner: sled::Iter,
}

impl ScanIterator {
    fn map(item: (sled::IVec, sled::IVec)) -> <Self as Iterator>::Item {
        let (key, value) = item;
        (key.to_vec(), value.to_vec())
    }
}

impl Iterator for ScanIterator {
    type Item = (Vec<u8>, Vec<u8>);

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next().map(|item| Self::map(item.unwrap()))
    }
}

impl DoubleEndedIterator for ScanIterator {
    fn next_back(&mut self) -> Option<Self::Item> {
        self.inner.next_back().map(|item| Self::map(item.unwrap()))
    }
}

#[cfg(test)]

mod test {
    use crate::storage::engine::StorageEngine;

    #[test]
    fn test_crud() {
        let mut engine = super::Memory::new();
        let _ = engine.set(b"hello", b"world".to_vec());
        let get = engine.get(b"hello");
        assert_eq!(get, Some(b"world".to_vec()));
    }
    #[test]
    fn test_recover() {
        let mut engine = super::Memory::new();
        let get = engine.get(b"hello");
        assert_eq!(get, Some(b"world".to_vec()));
        let get = engine.get(b"hello,world");
        assert_eq!(get, None);
    }
}
