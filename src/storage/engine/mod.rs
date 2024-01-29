pub mod memory;

pub trait StorageEngine: std::fmt::Display + Send + Sync {
    /// The iterator returned by scan(). Traits can't return "impl Trait", and
    /// we don't want to use trait objects, so the type must be specified.
    type ScanIterator: DoubleEndedIterator<Item = (Vec<u8>, Vec<u8>)>;

    /// Deletes a key, or does nothing if it does not exist.
    fn delete(&self, key: &[u8]) -> Result<(), StorageEngineError>;

    /// Flushes any buffered data to the underlying storage medium.
    fn flush(&self) -> Result<(), StorageEngineError>;

    /// Gets a value for a key, if it exists.
    fn get(&self, key: &[u8]) -> Option<Vec<u8>>;

    /// Iterates over an ordered range of key/value pairs.
    fn scan<R: std::ops::RangeBounds<Vec<u8>>>(&self, range: R) -> Self::ScanIterator;

    /// Sets a value for a key, replacing the existing value if any.
    fn set(&self, key: &[u8], value: Vec<u8>) -> Result<(), StorageEngineError>;

    /// Iterates over all key/value pairs starting with prefix.
    fn scan_prefix(&self, prefix: &[u8]) -> Self::ScanIterator {
        let start = std::ops::Bound::Included(prefix.to_vec());
        // let end = match prefix.iter().rposition(|b| *b != 0xff) {
        //     Some(i) => std::ops::Bound::Excluded(
        //         prefix
        //             .iter()
        //             .take(i)
        //             .copied()
        //             .chain(std::iter::once(prefix[i] + 1))
        //             .collect(),
        //     ),
        //     None => std::ops::Bound::Unbounded,
        // };
        let end = std::ops::Bound::Unbounded;
        self.scan((start, end))
     }
}
#[derive(Debug)]
pub enum StorageEngineError {
    Internal(String),
}
