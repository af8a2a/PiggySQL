use std::path::PathBuf;

use sled::Db;

use super::{KvScan, StorageEngine};
use crate::errors::Result;
pub struct SledStore {
    data: Db,
}
impl SledStore {
    pub fn new(path: PathBuf) -> Result<Self> {
        let db = sled::open(path)?;
        Ok(Self { data: db })
    }
}

impl StorageEngine for SledStore {
    fn delete(&self, key: &[u8]) -> Result<()> {
        self.data.remove(key)?;
        Ok(())
    }

    fn flush(&self) -> Result<()> {
        self.data.flush()?;
        Ok(())
    }

    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let val = self.data.get(key)?;
        match val {
            Some(val) => Ok(Some(val.to_vec())),
            None => Ok(None),
        }
    }

    fn scan(&self, range: impl std::ops::RangeBounds<Vec<u8>>) -> Result<KvScan> {
        Ok(Box::new(ScanIterator {
            inner: self.data.range(range),
            phantom_data: std::marker::PhantomData,
        }))
    }

    fn set(&self, key: &[u8], value: Vec<u8>) -> Result<()> {
        self.data.insert(key, value)?;
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
    phantom_data: std::marker::PhantomData<&'a ()>,
}

impl<'a> Iterator for ScanIterator<'a> {
    type Item = Result<(Vec<u8>, Vec<u8>)>;

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

    use std::sync::Arc;

    use super::*;
    super::super::tests::test_engine!({
        let path = tempdir::TempDir::new("piggydb")
            .unwrap()
            .path()
            .join("piggydb");
        Arc::new(SledStore::new(path)?)
    });
}
