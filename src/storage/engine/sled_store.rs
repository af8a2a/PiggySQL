use std::path::PathBuf;

use futures::future::ok;
use sled::Db;

use super::{StorageEngine, StorageEngineError};

pub struct SledStore {
    data: Db,
}
impl SledStore {
    pub fn new(path: PathBuf) -> Result<Self, StorageEngineError> {
        let db = sled::open(path)?;
        Ok(Self { data: db })
    }
}

impl StorageEngine for SledStore {
    type ScanIterator<'a> = ScanIterator<'a>;

    fn delete(&self, key: &[u8]) -> Result<(), super::StorageEngineError> {
        self.data.remove(key)?;
        Ok(())
    }

    fn flush(&self) -> Result<(), super::StorageEngineError> {
        self.data.flush()?;
        Ok(())
    }

    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, super::StorageEngineError> {
        let val = self.data.get(key)?;
        match val {
            Some(val) => Ok(Some(val.to_vec())),
            None => Ok(None),
        }
    }

    fn scan<R: std::ops::RangeBounds<Vec<u8>>>(&self, range: R) -> Self::ScanIterator<'_> {
        ScanIterator {
            inner: self.data.range(range),
            PhantomData: std::marker::PhantomData,
        }
    }

    fn set(&self, key: &[u8], value: Vec<u8>) -> Result<(), super::StorageEngineError> {
        self.data.insert(&key, value)?;
        Ok(())
    }
}

impl std::fmt::Display for SledStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "sled")
    }
}

pub struct ScanIterator<'a> {
    inner: sled::Iter,
    PhantomData: std::marker::PhantomData<&'a ()>,
}

impl<'a> Iterator for ScanIterator<'a> {
    type Item = Result<(Vec<u8>, Vec<u8>), StorageEngineError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner
            .next()
            .transpose()
            .unwrap()
            .map(|entry| Ok((entry.0.to_vec(), entry.1.to_vec())))
    }
}

impl<'a> DoubleEndedIterator for ScanIterator<'a> {
    fn next_back(&mut self) -> Option<Self::Item> {
        self.inner
            .next_back()
            .transpose()
            .unwrap()
            .map(|entry| Ok((entry.0.to_vec(), entry.1.to_vec())))
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    super::super::tests::test_engine!({
        
        let path = tempdir::TempDir::new("piggydb").unwrap().path().join("piggydb");
        SledStore::new(path)?
    });
}
