// use std::{
//     collections::HashSet, iter::Peekable, ops::{Bound, RangeBounds}, sync::Arc, vec
// };

// use bytes::Bytes;
// use itertools::Itertools;
// use serde::{Deserialize, Serialize};

// use super::engine::{KvScan, StorageEngine, StorageEngineError};
// type Version = u64;

// #[derive(Debug, Deserialize, Serialize)]
// pub enum Key {
//     /// The next available version.
//     NextVersion,
//     /// Active (uncommitted) transactions.
//     TxnActive(Version),
//     /// A snapshot of the active set at each version. Only written for
//     /// versions where the active set is non-empty (excluding itself).
//     TxnActiveSnapshot(Version),
//     /// Keeps track of all keys written to by an active transaction (identified
//     /// by its version), in case it needs to roll back.   
//     /// Write set for a transaction version.
//     TxnWrite(Version, Bytes),
//     /// A versioned key/value pair.
//     Version(Bytes, Version),
//     /// Unversioned non-transactional key/value pairs. These exist separately
//     /// from versioned keys, i.e. the unversioned key "foo" is entirely
//     /// independent of the versioned key "foo@7". These are mostly used
//     /// for metadata.
//     Unversioned(Bytes),
// }
// /// MVCC key prefixes, for prefix scans. These must match the keys above,
// /// including the enum variant index.
// #[derive(Debug, Deserialize, Serialize)]
// enum KeyPrefix {
//     NextVersion,
//     TxnActive,
//     TxnActiveSnapshot,
//     TxnWrite(Version),
//     Version(Bytes),
//     Unversioned,
// }
// impl KeyPrefix {
//     fn encode(&self) -> Result<Vec<u8>, bincode::Error> {
//         bincode::serialize(&self)
//     }
// }

// impl Key {
//     pub fn decode(bytes: &[u8]) -> Result<Self, bincode::Error> {
//         bincode::deserialize(bytes)
//     }

//     pub fn encode(&self) -> Result<Vec<u8>, bincode::Error> {
//         bincode::serialize(&self)
//     }
// }

// pub struct MVCCTransaction<E: StorageEngine> {
//     engine: Arc<E>,
//     state: TransactionState,
// }

// #[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
// pub struct TransactionState {
//     pub version: Version,
//     pub read_only: bool,
//     pub active: HashSet<Version>,
// }
// impl TransactionState {
//     fn is_visible(&self, version: Version) -> bool {
//         if self.active.contains(&version) {
//             false
//         } else if self.read_only {
//             version < self.version
//         } else {
//             version <= self.version
//         }
//     }
// }

// impl<E: StorageEngine> MVCCTransaction<E> {
//     pub fn begin(engine: Arc<E>) -> Result<Self, MVCCError> {
//         let version = match engine.get(&Key::NextVersion.encode().expect("get next version error"))
//         {
//             Some(ref v) => bincode::deserialize(v)?,
//             None => 1,
//         };
//         engine.set(
//             &Key::NextVersion.encode()?,
//             bincode::serialize(&(version + 1))?,
//         )?;
//         let active = Self::scan_active(engine.clone())?;
//         if !active.is_empty() {
//             engine.set(
//                 &Key::TxnActiveSnapshot(version).encode()?,
//                 bincode::serialize(&active)?,
//             )?
//         }
//         engine.set(&Key::TxnActive(version).encode()?, vec![])?;
//         Ok(Self {
//             engine,
//             state: TransactionState {
//                 version,
//                 read_only: false,
//                 active,
//             },
//         })
//     }
//     pub fn commit(&self) -> Result<(), MVCCError> {
//         if self.state.read_only {
//             return Ok(());
//         }
//         let perfix = self
//             .engine
//             .scan_prefix(&KeyPrefix::TxnWrite(self.state.version).encode()?)
//             .map(|(k, _)| k)
//             .collect_vec();
//         for key in perfix {
//             self.engine.delete(&key)?;
//         }
//         self.engine
//             .delete(&Key::TxnActive(self.state.version).encode()?)
//             .expect("delete txn active error");
//         Ok(())
//     }

//     pub fn rollback(&self) -> Result<(), MVCCError> {
//         if self.state.read_only {
//             return Ok(());
//         }
//         let write_set = self
//             .engine
//             .scan_prefix(&KeyPrefix::TxnWrite(self.state.version).encode()?)
//             .map(|(k, _)| k)
//             .collect_vec();
//         let mut rollback = Vec::new();
//         for key in write_set {
//             match Key::decode(&key)? {
//                 Key::TxnWrite(_, key) => {
//                     rollback.push(Key::Version(key, self.state.version).encode()?)
//                 }
//                 key => {
//                     return Err(MVCCError::KeyError(format!(
//                         "Expected TxnWrite, got:{:?}",
//                         key
//                     )));
//                 }
//             }
//         }
//         for key in rollback {
//             self.engine.delete(&key)?;
//         }
//         self.engine
//             .delete(&Key::TxnActive(self.state.version).encode()?);
//         Ok(())
//     }
//     pub fn delete(&self, key: &[u8]) -> Result<(), MVCCError> {
//         self.write_version(key, vec![])
//     }
//     pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, MVCCError> {
//         let from = Key::Version(Bytes::copy_from_slice(key), 0).encode()?;
//         let to = Key::Version(Bytes::copy_from_slice(key), self.state.version).encode()?;
//         let mut scan = self.engine.scan(from..=to).rev();
//         while let Some((key, value)) = scan.next() {
//             match Key::decode(&key)? {
//                 Key::Version(_, version) => {
//                     if self.state.is_visible(version) {
//                         return Ok(Some(value));
//                     }
//                 }
//                 key => {
//                     return Err(MVCCError::KeyError(format!(
//                         "Expected Key::Version got {:?}",
//                         key
//                     )))
//                 }
//             }
//         }
//         Ok(None)
//     }

//     fn write_version(&self, key: &[u8], value: Vec<u8>) -> Result<(), MVCCError> {
//         if !self.state.read_only {
//             return Err(MVCCError::ReadOnlyError("Read only".to_string()));
//         }
//         let from = Key::Version(
//             Bytes::copy_from_slice(key),
//             self.state
//                 .active
//                 .iter()
//                 .min()
//                 .copied()
//                 .unwrap_or(self.state.version + 1),
//         )
//         .encode()?;
//         let to = Key::Version(Bytes::copy_from_slice(key), Version::MAX).encode()?;
//         if let Some((key, _)) = self.engine.scan(from..=to).last() {
//             match Key::decode(&key)? {
//                 Key::Version(_, version) => {
//                     //被不可见事务修改
//                     if !self.state.is_visible(version) {
//                         return Err(MVCCError::Serialization);
//                     }
//                 }
//                 key => {
//                     return Err(MVCCError::InvalidVersion(format!("{:?}", key)));
//                 }
//             }
//         }
//         self.engine.set(
//             &Key::TxnWrite(self.state.version, Bytes::copy_from_slice(key)).encode()?,
//             vec![],
//         )?;
//         self.engine.set(
//             &Key::Version(Bytes::copy_from_slice(key), self.state.version).encode()?,
//             value,
//         )?;
//         Ok(())
//     }
//     fn scan_active(engine: Arc<E>) -> Result<HashSet<Version>, MVCCError> {
//         let scan = engine
//             .scan_prefix(&KeyPrefix::TxnActive.encode()?)
//             .map(|(k, _)| k)
//             .collect_vec();
//         let mut active = HashSet::new();
//         for key in scan {
//             match Key::decode(&key)? {
//                 Key::TxnActive(version) => active.insert(version),
//                 _ => {
//                     return Err(MVCCError::KeyError("Expected TxnActive".to_string()));
//                 }
//             };
//         }
//         Ok(active)  
//     }
//     pub fn scan<R: RangeBounds<Vec<u8>>>(&self, range: R) -> Result<KvScan, MVCCError> {
//         let start = match range.start_bound() {
//             Bound::Excluded(k) => {
//                 Bound::Excluded(Key::Version(Bytes::copy_from_slice(k), u64::MAX).encode()?)
//             }
//             Bound::Included(k) => {
//                 Bound::Included(Key::Version(Bytes::copy_from_slice(k), 0).encode()?)
//             }
//             Bound::Unbounded => Bound::Included(Key::Version(vec![].into(), 0).encode()?),
//         };
//         let end = match range.end_bound() {
//             Bound::Excluded(k) => {
//                 Bound::Excluded(Key::Version(Bytes::copy_from_slice(k), 0).encode()?)
//             }
//             Bound::Included(k) => {
//                 Bound::Included(Key::Version(Bytes::copy_from_slice(k), u64::MAX).encode()?)
//             }
//             Bound::Unbounded => Bound::Excluded(KeyPrefix::Unversioned.encode()?),
//         };
        
//         self.engine.scan(start..end).map(|iter|KvScan::n)
//     }
// }

// pub struct MvccScan {
//     /// The augmented KV store iterator, with key (decoded) and value. Note that we don't retain
//     /// the decoded version, so there will be multiple keys (for each version). We want the last.
//     scan: Peekable<KvScan>,
//     /// Keeps track of next_back() seen key, whose previous versions should be ignored.
//     next_back_seen: Option<Vec<u8>>,
// }
// impl Iterator for MvccScan {
//     type Item = Result<(Vec<u8>, Vec<u8>)>;

//     fn next(&mut self) -> Option<Self::Item> {
//         self.try_next().transpose()
//     }
// }

// impl MvccScan {
//     fn try_next(&mut self) -> Result<Option<(Vec<u8>, Vec<u8>)>> {
//         while let Some((key, value)) = self.scan.next() {
//             // Only return the item if it is the last version of the key.
//             if match self.scan.peek() {
//                 Some((peek_key, _)) if *peek_key != key => true,
//                 Some(_) => false,
//                 Some(Err(e)) => return Err(e.clone()),
//                 None => true,
//             } {
//                 // Only return non-deleted items.
//                 if let Some(value) = deserialize(&value)? {
//                     return Ok(Some((key, value)));
//                 }
//             }
//         }
//         Ok(None)
//     }

// }
// impl DoubleEndedIterator for MvccScan {
//     fn next_back(&mut self) -> Option<Self::Item> {
//         self.try_next_back().transpose()
//     }
// }



// #[derive(Debug)]
// pub enum MVCCError {
//     InvalidVersion(String),
//     EncodeError(String),
//     StorageError(String),
//     ReadOnlyError(String),
//     KeyError(String),
//     Serialization,
// }
// impl From<bincode::Error> for MVCCError {
//     fn from(e: bincode::Error) -> Self {
//         MVCCError::EncodeError(format!("{:?}", e))
//     }
// }
// impl From<StorageEngineError> for MVCCError {
//     fn from(e: StorageEngineError) -> Self {
//         MVCCError::StorageError(format!("{:?}", e))
//     }
// }
