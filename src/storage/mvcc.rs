use std::{
    collections::HashSet,
    iter::Peekable,
    ops::{Bound, RangeBounds},
    sync::Arc,
    vec,
};

use itertools::Itertools;
use kip_db::kernel::lsm::version;
use serde::{Deserialize, Serialize};

use super::{
    engine::{StorageEngine, StorageEngineError},
    keycode::{encode_bytes, encode_u64, take_bytes, take_u64},
};
type Version = u64;

#[derive(Debug, Deserialize, Serialize)]
pub enum Key {
    /// The next available version.
    NextVersion,
    /// Active (uncommitted) transactions.
    TxnActive(Version),
    /// A snapshot of the active set at each version. Only written for
    /// versions where the active set is non-empty (excluding itself).
    TxnActiveSnapshot(Version),
    /// Keeps track of all keys written to by an active transaction (identified
    /// by its version), in case it needs to roll back.   
    /// Write set for a transaction version.
    TxnWrite(Version, Vec<u8>),
    /// A versioned key/value pair.
    Version(Vec<u8>, Version),
    /// Unversioned non-transactional key/value pairs. These exist separately
    /// from versioned keys, i.e. the unversioned key "foo" is entirely
    /// independent of the versioned key "foo@7". These are mostly used
    /// for metadata.
    Unversioned(Vec<u8>),
}
/// MVCC key prefixes, for prefix scans. These must match the keys above,
/// including the enum variant index.
#[derive(Debug, Deserialize, Serialize)]
enum KeyPrefix {
    TxnWrite(Version),
    Version(Vec<u8>),
    Unversioned,
}
impl KeyPrefix {
    fn encode(&self) -> Result<Vec<u8>, MVCCError> {
        match self {
            KeyPrefix::TxnWrite(version) => Ok([&[0x04][..], &encode_u64(*version)].concat()),
            KeyPrefix::Version(key) => Ok([&[0x05][..], &encode_bytes(&key)].concat()),
            KeyPrefix::Unversioned => Ok(vec![0x06]),
        }
    }
}

impl Key {
    pub fn decode(bytes: &[u8]) -> Result<Self, MVCCError> {
        Ok(match bytes[0] {
            0x01 => Self::NextVersion,
            0x02 => Self::TxnActive(take_u64(bytes)?),
            0x03 => Self::TxnActiveSnapshot(take_u64(bytes)?),
            0x04 => Self::TxnWrite(take_u64(bytes)?, take_bytes(bytes)?.into()),
            0x05 => Self::Version(take_bytes(bytes)?.into(), take_u64(bytes)?),
            0x06 => Self::Unversioned(take_bytes(bytes)?.into()),
            _ => {
                return Err(MVCCError::Serialization(format!(
                    "Invalid key prefix {:?}",
                    bytes[0]
                )))
            }
        })
    }

    pub fn encode(&self) -> Result<Vec<u8>, MVCCError> {
        match self {
            Key::NextVersion => Ok(vec![0x01]),
            Key::TxnActive(version) => Ok([&[0x02][..], &encode_u64(*version)].concat()),
            Key::TxnActiveSnapshot(version) => Ok([&[0x03][..], &encode_u64(*version)].concat()),
            Key::TxnWrite(version, key) => {
                Ok([&[0x04][..], &encode_u64(*version), &encode_bytes(&key)].concat())
            }
            Key::Version(key, version) => {
                Ok([&[0x05][..], &encode_bytes(&key), &encode_u64(*version)].concat())
            }
            Key::Unversioned(key) => Ok([&[0x06][..], &encode_bytes(&key)].concat()),
        }
    }
}

pub struct MVCCTransaction<E: StorageEngine> {
    engine: Arc<E>,
    state: TransactionState,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct TransactionState {
    pub version: Version,
    pub read_only: bool,
    pub active: HashSet<Version>,
}
impl TransactionState {
    fn is_visible(&self, version: Version) -> bool {
        if self.active.contains(&version) {
            false
        } else if self.read_only {
            version < self.version
        } else {
            version <= self.version
        }
    }
}
impl<E: StorageEngine> MVCCTransaction<E> {
    pub fn begin(engine: Arc<E>) -> Result<MVCCTransaction<E>, MVCCError> {
        todo!()
    }
}

struct VersionIterator<'a, E: StorageEngine + 'a> {
    txn: &'a TransactionState,
    inner: E::ScanIterator<'a>,
}

impl<'a, E: StorageEngine + 'a> VersionIterator<'a, E> {
    fn new(txn: &'a TransactionState, inner: E::ScanIterator<'a>) -> Self {
        Self { txn, inner }
    }
    /// Decodes a raw engine key into an MVCC key and version, returning None if
    /// the version is not visible.
    fn decode_visible(&self, key: &[u8]) -> Result<Option<(Vec<u8>, Version)>, MVCCError> {
        let (key, version) = match Key::decode(key)? {
            Key::Version(key, version) => (key.to_vec(), version),
            key => {
                return Err(MVCCError::KeyError(format!(
                    "Expected Key::Version got {:?}",
                    key
                )))
            }
        };
        if self.txn.is_visible(version) {
            Ok(Some((key, version)))
        } else {
            Ok(None)
        }
    }
    // Fallible next(), emitting the next item, or None if exhausted.
    fn try_next(&mut self) -> Result<Option<(Vec<u8>, Version, Vec<u8>)>, MVCCError> {
        while let Some((key, value)) = self.inner.next().transpose()? {
            if let Some((key, version)) = self.decode_visible(&key)? {
                return Ok(Some((key, version, value)));
            }
        }
        Ok(None)
    }
    // Fallible next_back(), emitting the previous item, or None if exhausted.
    fn try_next_back(&mut self) -> Result<Option<(Vec<u8>, Version, Vec<u8>)>, MVCCError> {
        while let Some((key, value)) = self.inner.next_back().transpose()? {
            if let Some((key, version)) = self.decode_visible(&key)? {
                return Ok(Some((key, version, value)));
            }
        }
        Ok(None)
    }
}

impl<'a, E: StorageEngine> Iterator for VersionIterator<'a, E> {
    type Item = Result<(Vec<u8>, Version, Vec<u8>), MVCCError>;
    fn next(&mut self) -> Option<Self::Item> {
        self.try_next().transpose()
    }
}

impl<'a, E: StorageEngine> DoubleEndedIterator for VersionIterator<'a, E> {
    fn next_back(&mut self) -> Option<Self::Item> {
        self.try_next_back().transpose()
    }
}

#[derive(Clone, Debug)]
pub struct MVCC<E: StorageEngine> {
    engine: Arc<E>,
}
impl<E: StorageEngine> MVCC<E> {
    pub fn new(engine: Arc<E>) -> Self {
        Self { engine }
    }
    pub fn begin(&self) -> Result<MVCCTransaction<E>, MVCCError> {
        MVCCTransaction::begin(self.engine.clone())
    }

    pub fn get_unversioned(&self, key: &[u8]) -> Result<Option<Vec<u8>>, MVCCError> {
        self.engine
            .get(&Key::Unversioned(key.to_vec()).encode()?)
            .map_err(|e| MVCCError::from(e))
    }
    pub fn set_unversioned(&self, key: &[u8], value: Vec<u8>) -> Result<(), MVCCError> {
        self.engine
            .set(&Key::Unversioned(key.into()).encode()?, value)
            .map_err(|e| MVCCError::from(e))
    }
}

#[derive(Debug)]
pub enum MVCCError {
    InvalidVersion(String),
    EncodeError(String),
    StorageError(String),
    ReadOnlyError(String),
    KeyError(String),
    Serialization(String),
}
impl From<bincode::Error> for MVCCError {
    fn from(e: bincode::Error) -> Self {
        MVCCError::EncodeError(format!("{:?}", e))
    }
}
impl From<StorageEngineError> for MVCCError {
    fn from(e: StorageEngineError) -> Self {
        MVCCError::StorageError(format!("{:?}", e))
    }
}
impl From<std::array::TryFromSliceError> for MVCCError {
    fn from(err: std::array::TryFromSliceError) -> Self {
        MVCCError::Serialization(err.to_string())
    }
}

impl From<std::string::FromUtf8Error> for MVCCError {
    fn from(err: std::string::FromUtf8Error) -> Self {
        MVCCError::Serialization(err.to_string())
    }
}
