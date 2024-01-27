use std::collections::BTreeMap;

use bytes::Bytes;
use serde::{Deserialize, Serialize};



pub trait StorageEngine: std::fmt::Display + Send + Sync {
    /// The iterator returned by scan(). Traits can't return "impl Trait", and
    /// we don't want to use trait objects, so the type must be specified.
    type ScanIterator<'a>: DoubleEndedIterator<Item = Result<(Bytes, Bytes),StoreError>> + 'a
    where
        Self: 'a;

    /// Deletes a key, or does nothing if it does not exist.
    fn delete(&mut self, key: &[u8]) -> Result<(),StoreError>;

    /// Flushes any buffered data to the underlying storage medium.
    fn flush(&mut self) -> Result<(),StoreError>;

    /// Gets a value for a key, if it exists.
    fn get(&mut self, key: &[u8]) -> Result<Option<Bytes>,StoreError>;

    /// Iterates over an ordered range of key/value pairs.
    fn scan<R: std::ops::RangeBounds<Bytes>>(&mut self, range: R) -> Self::ScanIterator<'_>;

    /// Sets a value for a key, replacing the existing value if any.
    fn set(&mut self, key: &[u8], value: Bytes) -> Result<(),StoreError>;

    /// Iterates over all key/value pairs starting with prefix.
    fn scan_prefix(&mut self, prefix: &[u8]) -> Self::ScanIterator<'_> {
        let start = std::ops::Bound::Included(prefix.to_vec());
        let end = match prefix.iter().rposition(|b| *b != 0xff) {
            Some(i) => std::ops::Bound::Excluded(
                prefix
                    .iter()
                    .take(i)
                    .copied()
                    .chain(std::iter::once(prefix[i] + 1))
                    .collect(),
            ),
            None => std::ops::Bound::Unbounded,
        };
        self.scan((Bytes::from(start), Bytes::from(end)))
    }
}

pub struct MockStorage{
    storage: BTreeMap<Bytes,Bytes>
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum StoreError {
    Internal(String),
}