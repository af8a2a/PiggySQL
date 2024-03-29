pub mod txn;
mod watermark;

use std::{
    collections::{BTreeMap, HashSet},
    sync::{atomic::AtomicBool, Arc},
};

use crossbeam_skiplist::SkipMap;
use parking_lot::Mutex;

use self::{txn::Transaction, watermark::Watermark};

use super::lsm_storage::LsmStorageInner;

pub(crate) struct CommittedTxnData {
    pub(crate) key_hashes: HashSet<u32>,
    #[allow(dead_code)]
    pub(crate) read_ts: u64,
    #[allow(dead_code)]
    pub(crate) commit_ts: u64,
}

pub(crate) struct LsmMvccInner {
    pub(crate) write_lock: Mutex<()>,
    pub(crate) ts: Arc<Mutex<(u64, Watermark)>>,
    pub(crate) committed_txns: Arc<Mutex<BTreeMap<u64, CommittedTxnData>>>,
}

impl LsmMvccInner {
    pub fn new(initial_ts: u64) -> Self {
        Self {
            write_lock: Mutex::new(()),
            ts: Arc::new(Mutex::new((initial_ts, Watermark::new()))),
            committed_txns: Arc::new(Mutex::new(BTreeMap::new())),
        }
    }

    pub fn latest_commit_ts(&self) -> u64 {
        self.ts.lock().0
    }

    pub fn update_commit_ts(&self, ts: u64) {
        self.ts.lock().0 = ts;
    }

    /// All ts (strictly) below this ts can be garbage collected.
    pub fn watermark(&self) -> u64 {
        let ts = self.ts.lock();
        ts.1.watermark().unwrap_or(ts.0)
    }

    pub fn new_txn(&self, inner: Arc<LsmStorageInner>, serializable: bool) -> Arc<Transaction> {
        let ts = self.ts.lock();
        let read_ts = ts.0;
        Arc::new(Transaction {
            inner,
            read_ts,
            local_storage: Arc::new(SkipMap::new()),
            committed: Arc::new(AtomicBool::new(false)),
            key_hashes: None,
        })
    }
}
