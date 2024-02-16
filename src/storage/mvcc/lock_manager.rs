use crate::errors::*;
use dashmap::DashMap;
use std::collections::HashSet;

#[derive(Clone, Copy)]
struct TxnStatus {
    /// The flag for an RW-depenedncy from another txn to this txn.
    in_conflict: bool,
    /// The flag for an RW-depenedncy from this txn to another txn.
    out_conflict: bool,
    /// The commit timestamp (current MvccKey::TxnNext) for this txn.
    /// None if it is still running.
    commit_timestamp: Option<u64>,
}

/// A lock manager for Serializable Snapshot Isolation.
#[derive(Clone)]
pub(super) struct LockManager {
    read_locks: DashMap<Vec<u8>, HashSet<u64>>,
    write_locks: DashMap<Vec<u8>, HashSet<u64>>,
    txn_status: DashMap<u64, TxnStatus>,
}

impl LockManager {
    /// Creates a new lock manager.
    pub(super) fn new() -> Self {
        Self {
            read_locks: DashMap::new(),
            write_locks: DashMap::new(),
            txn_status: DashMap::new(),
        }
    }

    /// Initializes the transaction status with negative flags and uncommitted.
    pub(super) fn init_txn(&self, txn_id: u64) {
        self.txn_status.insert(
            txn_id,
            TxnStatus {
                in_conflict: false,
                out_conflict: false,
                commit_timestamp: None,
            },
        );
    }

    /// Releases all SIREAD locks acquired by the transaction.
    fn release_read_lock(&self, txn_id: u64) {
        self.read_locks.alter_all(|_, mut readers| {
            readers.remove(&txn_id);
            readers
        });
    }

    /// Releases all WRITE locks acquired by the transaction.
    fn release_write_lock(&self, txn_id: u64) {
        self.write_locks.alter_all(|_, mut writers| {
            writers.remove(&txn_id);
            writers
        });
    }

    /// Changes the transaction status to committed, releasing all WRITE locks.
    pub(super) fn commit_txn(&self, txn_id: u64, commit_timestamp: u64) -> Result<()> {
        self.txn_status
            .get_mut(&txn_id)
            .ok_or(DatabaseError::InternalError(format!(
                "Expected status of txn {} in SSI manager.",
                txn_id
            )))?
            .value_mut()
            .commit_timestamp = Some(commit_timestamp);

        self.release_write_lock(txn_id);

        Ok(())
    }

    /// Remove the transaction status and release both SIREAD and WRITE locks.
    pub(super) fn rollback_txn(&self, txn_id: u64) {
        self.release_read_lock(txn_id);
        self.release_write_lock(txn_id);
        self.txn_status.remove(&txn_id);
    }

    /// Acquires the SIREAD lock on the object for the transaction.
    pub(super) fn acquire_read_lock(&self, key: Vec<u8>, owner_id: u64) {
        self.read_locks.get_mut(&key).map_or_else(
            || {
                self.read_locks.insert(key, HashSet::from([owner_id]));
            },
            |mut entry| {
                entry.value_mut().insert(owner_id);
            },
        );
    }

    /// Acquires the WRITE lock on the object for the transaction.
    pub(super) fn acquire_write_lock(&self, key: Vec<u8>, owner_id: u64) {
        self.write_locks.get_mut(&key).map_or_else(
            || {
                self.write_locks.insert(key, HashSet::from([owner_id]));
            },
            |mut entry| {
                entry.value_mut().insert(owner_id);
            },
        );
    }

    /// Checks all acquired SIREAD locks on the object, recording an RW-denpendency each.
    /// Aborts the transaction if necessary.
    pub(super) fn check_read_locks(&self, key: Vec<u8>, txn_id: u64) -> Result<()> {
        if let Some(entry) = self.read_locks.get(&key) {
            for owner_id in entry.value().to_owned().into_iter() {
                if owner_id == txn_id {
                    continue;
                }
                let status = self
                    .txn_status
                    .get(&owner_id)
                    .ok_or(DatabaseError::InternalError(format!(
                        "Expected status of txn {} in SSI manager.",
                        owner_id
                    )))?
                    .value()
                    .clone();
                match status.commit_timestamp {
                    Some(time) if time > txn_id => {
                        if status.in_conflict {
                            return Err(DatabaseError::Serialization);
                        }
                    }
                    None => self.record_conflict(owner_id, txn_id)?,
                    _ => {}
                }
            }
            self.check_abort(txn_id)?;
        }
        Ok(())
    }

    /// Checks all acquired WRITE locks on the object, recording an RW-denpendency each.
    pub(super) fn check_write_locks(&self, key: Vec<u8>, txn_id: u64) -> Result<()> {
        if let Some(entry) = self.read_locks.get(&key) {
            for owner_id in entry.value().to_owned().into_iter() {
                if owner_id == txn_id {
                    continue;
                }
                self.record_conflict(txn_id, owner_id)?
            }
            self.check_abort(txn_id)?;
        }
        Ok(())
    }

    /// Records the RW-dependency with the transaction and the creator of newer-versioned entry.
    /// Aborts the transaction if necessary.
    pub(super) fn abort_or_record_conflict(&self, creator_id: u64, txn_id: u64) -> Result<()> {
        let status = self
            .txn_status
            .get(&creator_id)
            .ok_or(DatabaseError::InternalError(format!(
                "Expected status of txn {} in SSI manager.",
                creator_id
            )))?
            .value()
            .clone();
        match status.commit_timestamp {
            Some(_) if status.out_conflict => return Err(DatabaseError::Serialization),
            _ => self.record_conflict(txn_id, creator_id)?,
        }
        self.check_abort(txn_id)
    }

    /// Records an RW-dependency from `out_id`(R) to `in_id`(W).
    pub(super) fn record_conflict(&self, out_id: u64, in_id: u64) -> Result<()> {
        self.txn_status
            .get_mut(&out_id)
            .ok_or(DatabaseError::InternalError(format!(
                "Expected status of txn {} in SSI manager.",
                out_id
            )))?
            .value_mut()
            .out_conflict = true;
        self.txn_status
            .get_mut(&in_id)
            .ok_or(DatabaseError::InternalError(format!(
                "Expected status of txn {} in SSI manager.",
                in_id
            )))?
            .value_mut()
            .in_conflict = true;
        Ok(())
    }

    /// Aborts the transaction if it has become a pivot (both flags are positive).
    /// Does this by returning `Error::Serialization`.
    pub(super) fn check_abort(&self, txn_id: u64) -> Result<()> {
        let status = self
            .txn_status
            .get(&txn_id)
            .ok_or(DatabaseError::InternalError(format!(
                "Expected status of txn {} in SSI manager.",
                txn_id
            )))?
            .value()
            .clone();
        match *&status.in_conflict && *&status.out_conflict {
            true => Err(DatabaseError::Serialization),
            false => Ok(()),
        }
    }
}
