use std::sync::Arc;

use crossbeam_skiplist::SkipMap;

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
        Ok(Box::new(
            self.data
                .range((range.start_bound().cloned(), range.end_bound().cloned()))
                .map(|entry| Ok((entry.key().clone(), entry.value().clone())))
                .collect::<Vec<_>>()
                .into_iter(),
        ))
    }

    fn set(&self, key: &[u8], value: Vec<u8>) -> Result<()> {
        self.data.insert(key.to_vec(), value);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    super::super::tests::test_engine!(Arc::new(Memory::new()));
}
