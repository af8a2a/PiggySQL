use tracing::debug;

use super::{lsm_storage::LsmStorageInner, PiggyKV};

impl LsmStorageInner {
    pub fn dump_structure(&self) {
        let snapshot = self.state.read();
        if !snapshot.l0_sstables.is_empty() {
            debug!(
                "L0 ({}): {:?}",
                snapshot.l0_sstables.len(),
                snapshot.l0_sstables,
            );
        }
        for (level, files) in &snapshot.levels {
            debug!("L{level} ({}): {:?}", files.len(), files);
        }
    }
}

impl PiggyKV {
    pub fn dump_structure(&self) {
        self.inner.dump_structure()
    }
}
