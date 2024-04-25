mod leveled;
use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use crate::errors::Result;
pub use leveled::{LeveledCompactionController, LeveledCompactionOptions, LeveledCompactionTask};
use serde::{Deserialize, Serialize};
use tracing::debug;

use super::iterators::concat_iterator::SstConcatIterator;
use super::iterators::merge_iterator::MergeIterator;
use super::iterators::two_merge_iterator::TwoMergeIterator;
use super::iterators::StorageIterator;
use super::key::KeySlice;
use super::lsm_storage::{LsmStorageInner, LsmStorageState};
use super::manifest::ManifestRecord;
use super::table::{SsTable, SsTableBuilder, SsTableIterator};

#[derive(Debug, Serialize, Deserialize)]
pub enum CompactionTask {
    Leveled(LeveledCompactionTask),
    ForceFullCompaction {
        l0_sstables: Vec<usize>,
        l1_sstables: Vec<usize>,
    },
}

impl CompactionTask {
    fn compact_to_bottom_level(&self) -> bool {
        match self {
            CompactionTask::ForceFullCompaction { .. } => true,
            CompactionTask::Leveled(task) => task.is_lower_level_bottom_level,
        }
    }
}

pub(crate) enum CompactionController {
    Leveled(LeveledCompactionController),
    NoCompaction,
}

impl CompactionController {
    pub fn generate_compaction_task(&self, snapshot: &LsmStorageState) -> Option<CompactionTask> {
        match self {
            CompactionController::Leveled(ctrl) => ctrl
                .generate_compaction_task(snapshot)
                .map(CompactionTask::Leveled),
            CompactionController::NoCompaction => unreachable!(),
        }
    }

    pub fn apply_compaction_result(
        &self,
        snapshot: &LsmStorageState,
        task: &CompactionTask,
        output: &[usize],
        in_recovery: bool,
    ) -> (LsmStorageState, Vec<usize>) {
        match (self, task) {
            (CompactionController::Leveled(ctrl), CompactionTask::Leveled(task)) => {
                ctrl.apply_compaction_result(snapshot, task, output, in_recovery)
            }
            _ => unreachable!(),
        }
    }
}

impl CompactionController {
    pub fn flush_to_l0(&self) -> bool {
        matches!(self, Self::Leveled(_) | Self::NoCompaction)
    }
}

#[derive(Debug, Clone)]
pub enum CompactionOptions {
    /// Leveled compaction with partial compaction + dynamic level support (= RocksDB's Leveled
    /// Compaction)
    Leveled(LeveledCompactionOptions),
    /// In no compaction mode (week 1), always flush to L0
    NoCompaction,
}

impl LsmStorageInner {
    fn compact_generate_sst_from_iter(
        &self,
        mut iter: impl for<'a> StorageIterator<KeyType<'a> = KeySlice<'a>>,
        compact_to_bottom_level: bool,
    ) -> Result<Vec<Arc<SsTable>>> {
        let mut builder = None;
        let mut new_sst = Vec::new();
        let mut last_key = Vec::<u8>::new();
        let watermark = self.mvcc().watermark();
        let mut first_key_below_watermark = false;

        while iter.is_valid() {
            if builder.is_none() {
                builder = Some(SsTableBuilder::new(self.options.block_size));
            }
            let same_as_last_key = iter.key().key_ref() == last_key;
            if !same_as_last_key {
                first_key_below_watermark = true;
            }

            if compact_to_bottom_level
                && !same_as_last_key
                && iter.key().ts() <= watermark
                && iter.value().is_empty()
            {
                last_key.clear();
                last_key.extend(iter.key().key_ref());
                iter._next()?;
                first_key_below_watermark = false;
                continue;
            }

            if same_as_last_key && iter.key().ts() <= watermark {
                if !first_key_below_watermark {
                    iter._next()?;
                    continue;
                }
                first_key_below_watermark = false;
            }

            let builder_inner = builder.as_mut().unwrap();
            if builder_inner.estimated_size() >= self.options.target_sst_size {
                let sst_id = self.next_sst_id();
                let old_builder = builder.take().unwrap();
                let sst = Arc::new(old_builder.build(
                    sst_id,
                    Some(self.block_cache.clone()),
                    self.options.bloom_false_positive_rate,
                    self.path_of_sst(sst_id),
                )?);
                new_sst.push(sst);
                builder = Some(SsTableBuilder::new(self.options.block_size));
            }
            let builder_inner = builder.as_mut().unwrap();
            builder_inner.add(iter.key(), iter.value());

            if !same_as_last_key {
                last_key.clear();
                last_key.extend(iter.key().key_ref());
            }

            iter._next()?;
        }
        if let Some(builder) = builder {
            let sst_id = self.next_sst_id(); // lock dropped here
            let sst = Arc::new(builder.build(
                sst_id,
                Some(self.block_cache.clone()),
                self.options.bloom_false_positive_rate,
                self.path_of_sst(sst_id),
            )?);
            new_sst.push(sst);
        }
        Ok(new_sst)
    }

    fn compact(&self, task: &CompactionTask) -> Result<Vec<Arc<SsTable>>> {
        let snapshot = {
            let state = self.state.read();
            state.clone()
        };
        match task {
            CompactionTask::ForceFullCompaction {
                l0_sstables,
                l1_sstables,
            } => {
                let mut l0_iters = Vec::with_capacity(l0_sstables.len());
                for id in l0_sstables.iter() {
                    l0_iters.push(Box::new(SsTableIterator::create_and_seek_to_first(
                        snapshot.sstables.get(id).unwrap().clone(),
                    )?));
                }
                let mut l1_iters = Vec::with_capacity(l1_sstables.len());
                for id in l1_sstables.iter() {
                    l1_iters.push(snapshot.sstables.get(id).unwrap().clone());
                }
                let iter = TwoMergeIterator::create(
                    MergeIterator::create(l0_iters),
                    SstConcatIterator::create_and_seek_to_first(l1_iters)?,
                )?;
                self.compact_generate_sst_from_iter(iter, task.compact_to_bottom_level())
            }
            CompactionTask::Leveled(LeveledCompactionTask {
                upper_level,
                upper_level_sst_ids,
                lower_level: _,
                lower_level_sst_ids,
                ..
            }) => match upper_level {
                Some(_) => {
                    let mut upper_ssts = Vec::with_capacity(upper_level_sst_ids.len());
                    for id in upper_level_sst_ids.iter() {
                        upper_ssts.push(snapshot.sstables.get(id).unwrap().clone());
                    }
                    let upper_iter = SstConcatIterator::create_and_seek_to_first(upper_ssts)?;
                    let mut lower_ssts = Vec::with_capacity(lower_level_sst_ids.len());
                    for id in lower_level_sst_ids.iter() {
                        lower_ssts.push(snapshot.sstables.get(id).unwrap().clone());
                    }
                    let lower_iter = SstConcatIterator::create_and_seek_to_first(lower_ssts)?;
                    self.compact_generate_sst_from_iter(
                        TwoMergeIterator::create(upper_iter, lower_iter)?,
                        task.compact_to_bottom_level(),
                    )
                }
                None => {
                    let mut upper_iters = Vec::with_capacity(upper_level_sst_ids.len());
                    for id in upper_level_sst_ids.iter() {
                        upper_iters.push(Box::new(SsTableIterator::create_and_seek_to_first(
                            snapshot.sstables.get(id).unwrap().clone(),
                        )?));
                    }
                    let upper_iter = MergeIterator::create(upper_iters);
                    let mut lower_ssts = Vec::with_capacity(lower_level_sst_ids.len());
                    for id in lower_level_sst_ids.iter() {
                        lower_ssts.push(snapshot.sstables.get(id).unwrap().clone());
                    }
                    let lower_iter = SstConcatIterator::create_and_seek_to_first(lower_ssts)?;
                    self.compact_generate_sst_from_iter(
                        TwoMergeIterator::create(upper_iter, lower_iter)?,
                        task.compact_to_bottom_level(),
                    )
                }
            },
        }
    }

    pub fn force_full_compaction(&self) -> Result<()> {
        let CompactionOptions::NoCompaction = self.options.compaction_options else {
            panic!("full compaction can only be called with compaction is not enabled")
        };

        let snapshot = {
            let state = self.state.read();
            state.clone()
        };

        let l0_sstables = snapshot.l0_sstables.clone();
        let l1_sstables = snapshot.levels[0].1.clone();
        let compaction_task = CompactionTask::ForceFullCompaction {
            l0_sstables: l0_sstables.clone(),
            l1_sstables: l1_sstables.clone(),
        };

        println!("force full compaction: {:?}", compaction_task);

        let sstables = self.compact(&compaction_task)?;
        let mut ids = Vec::with_capacity(sstables.len());

        {
            let state_lock = self.state_lock.lock();
            let mut state = self.state.read().as_ref().clone();
            for sst in l0_sstables.iter().chain(l1_sstables.iter()) {
                let result = state.sstables.remove(sst);
                assert!(result.is_some());
            }
            for new_sst in sstables {
                ids.push(new_sst.sst_id());
                let result = state.sstables.insert(new_sst.sst_id(), new_sst);
                assert!(result.is_none());
            }
            assert_eq!(l1_sstables, state.levels[0].1);
            state.levels[0].1 = ids.clone();
            let mut l0_sstables_map = l0_sstables.iter().copied().collect::<HashSet<_>>();
            state.l0_sstables = state
                .l0_sstables
                .iter()
                .filter(|x| !l0_sstables_map.remove(x))
                .copied()
                .collect::<Vec<_>>();
            assert!(l0_sstables_map.is_empty());
            *self.state.write() = Arc::new(state);
            self.sync_dir()?;
            self.manifest.as_ref().unwrap().add_record(
                &state_lock,
                ManifestRecord::Compaction(compaction_task, ids.clone()),
            )?;
        }
        for sst in l0_sstables.iter().chain(l1_sstables.iter()) {
            std::fs::remove_file(self.path_of_sst(*sst))?;
        }

        println!("force full compaction done, new SSTs: {:?}", ids);

        Ok(())
    }

    fn trigger_compaction(&self) -> Result<()> {
        let snapshot = {
            let state = self.state.read();
            state.clone()
        };
        let task = self
            .compaction_controller
            .generate_compaction_task(&snapshot);
        let Some(task) = task else {
            return Ok(());
        };
        // self.dump_structure();
        debug!("running compaction task: {:?}", task);
        let sstables = self.compact(&task)?;
        let output = sstables.iter().map(|x| x.sst_id()).collect::<Vec<_>>();
        let ssts_to_remove = {
            let state_lock = self.state_lock.lock();
            let mut snapshot = self.state.read().as_ref().clone();
            let mut new_sst_ids = Vec::new();
            for file_to_add in sstables {
                new_sst_ids.push(file_to_add.sst_id());
                let result = snapshot.sstables.insert(file_to_add.sst_id(), file_to_add);
                assert!(result.is_none());
            }
            let (mut snapshot, files_to_remove) = self
                .compaction_controller
                .apply_compaction_result(&snapshot, &task, &output, false);
            let mut ssts_to_remove = Vec::with_capacity(files_to_remove.len());
            for file_to_remove in &files_to_remove {
                let result = snapshot.sstables.remove(file_to_remove);
                assert!(result.is_some(), "cannot remove {}.sst", file_to_remove);
                ssts_to_remove.push(result.unwrap());
            }
            let mut state = self.state.write();
            *state = Arc::new(snapshot);
            drop(state);
            self.sync_dir()?;
            self.manifest()
                .add_record(&state_lock, ManifestRecord::Compaction(task, new_sst_ids))?;
            ssts_to_remove
        };
        debug!(
            "compaction finished: {} files removed, {} files added, output={:?}",
            ssts_to_remove.len(),
            output.len(),
            output
        );
        for sst in ssts_to_remove {
            std::fs::remove_file(self.path_of_sst(sst.sst_id()))?;
        }
        self.sync_dir()?;

        Ok(())
    }

    pub(crate) fn spawn_compaction_thread(
        self: &Arc<Self>,
        rx: crossbeam_channel::Receiver<()>,
    ) -> Result<Option<std::thread::JoinHandle<()>>> {
        if let CompactionOptions::Leveled(_) = self.options.compaction_options {
            let this = self.clone();
            let handle = std::thread::spawn(move || {
                let ticker = crossbeam_channel::tick(Duration::from_millis(50));
                loop {
                    crossbeam_channel::select! {
                        recv(ticker) -> _ => if let Err(e) = this.trigger_compaction() {
                            eprintln!("compaction failed: {}", e);
                        },
                        recv(rx) -> _ => return
                    }
                }
            });
            return Ok(Some(handle));
        }
        Ok(None)
    }

    fn trigger_flush(&self) -> Result<()> {
        let res = {
            let state = self.state.read();
            state.imm_memtables.len() >= self.options.num_memtable_limit
        };
        if res {
            self.force_flush_next_imm_memtable()?;
        }

        Ok(())
    }

    pub(crate) fn spawn_flush_thread(
        self: &Arc<Self>,
        rx: crossbeam_channel::Receiver<()>,
    ) -> Result<Option<std::thread::JoinHandle<()>>> {
        let this = self.clone();
        let handle = std::thread::spawn(move || {
            let ticker = crossbeam_channel::tick(Duration::from_millis(50));
            loop {
                crossbeam_channel::select! {
                    recv(ticker) -> _ => if let Err(e) = this.trigger_flush() {
                        eprintln!("flush failed: {}", e);
                    },
                    recv(rx) -> _ => return
                }
            }
        });
        Ok(Some(handle))
    }
}


#[cfg(test)]
mod test {
    use std::{ops::Bound, path::Path, sync::Arc};

    use bytes::Bytes;
    use tempfile::tempdir;
    use crate::storage::engine::piggykv::{debug::{check_iter_result_by_key, check_lsm_iter_result_by_key, construct_merge_iterator_over_storage, sync}, iterators::{concat_iterator::SstConcatIterator, StorageIterator}, key::{KeySlice, TS_ENABLED}, lsm_storage::{LsmStorageInner, LsmStorageOptions}, table::{bloom::Bloom, FileObject, SsTable, SsTableBuilder}};
    #[test]
    fn test_task1_full_compaction() {
        // We do not use LSM iterator in this test because it's implemented as part of task 3
        let dir = tempdir().unwrap();
        let storage =
            Arc::new(LsmStorageInner::open(&dir, LsmStorageOptions::default()).unwrap());
        #[allow(clippy::let_unit_value)]
        let _txn = storage.new_txn().unwrap();
        storage.put(b"0", b"v1").unwrap();
        sync(&storage);
        storage.put(b"0", b"v2").unwrap();
        storage.put(b"1", b"v2").unwrap();
        storage.put(b"2", b"v2").unwrap();
        sync(&storage);
        storage.delete(b"0").unwrap();
        storage.delete(b"2").unwrap();
        sync(&storage);
        assert_eq!(storage.state.read().l0_sstables.len(), 3);
        let mut iter = construct_merge_iterator_over_storage(&storage.state.read());
        if TS_ENABLED {
            check_iter_result_by_key(
                &mut iter,
                vec![
                    (Bytes::from_static(b"0"), Bytes::from_static(b"")),
                    (Bytes::from_static(b"0"), Bytes::from_static(b"v2")),
                    (Bytes::from_static(b"0"), Bytes::from_static(b"v1")),
                    (Bytes::from_static(b"1"), Bytes::from_static(b"v2")),
                    (Bytes::from_static(b"2"), Bytes::from_static(b"")),
                    (Bytes::from_static(b"2"), Bytes::from_static(b"v2")),
                ],
            );
        } else {
            check_iter_result_by_key(
                &mut iter,
                vec![
                    (Bytes::from_static(b"0"), Bytes::from_static(b"")),
                    (Bytes::from_static(b"1"), Bytes::from_static(b"v2")),
                    (Bytes::from_static(b"2"), Bytes::from_static(b"")),
                ],
            );
        }
        storage.force_full_compaction().unwrap();
        assert!(storage.state.read().l0_sstables.is_empty());
        let mut iter = construct_merge_iterator_over_storage(&storage.state.read());
        if TS_ENABLED {
            check_iter_result_by_key(
                &mut iter,
                vec![
                    (Bytes::from_static(b"0"), Bytes::from_static(b"")),
                    (Bytes::from_static(b"0"), Bytes::from_static(b"v2")),
                    (Bytes::from_static(b"0"), Bytes::from_static(b"v1")),
                    (Bytes::from_static(b"1"), Bytes::from_static(b"v2")),
                    (Bytes::from_static(b"2"), Bytes::from_static(b"")),
                    (Bytes::from_static(b"2"), Bytes::from_static(b"v2")),
                ],
            );
        } else {
            check_iter_result_by_key(
                &mut iter,
                vec![(Bytes::from_static(b"1"), Bytes::from_static(b"v2"))],
            );
        }
        storage.put(b"0", b"v3").unwrap();
        storage.put(b"2", b"v3").unwrap();
        sync(&storage);
        storage.delete(b"1").unwrap();
        sync(&storage);
        let mut iter = construct_merge_iterator_over_storage(&storage.state.read());
        if TS_ENABLED {
            check_iter_result_by_key(
                &mut iter,
                vec![
                    (Bytes::from_static(b"0"), Bytes::from_static(b"v3")),
                    (Bytes::from_static(b"0"), Bytes::from_static(b"")),
                    (Bytes::from_static(b"0"), Bytes::from_static(b"v2")),
                    (Bytes::from_static(b"0"), Bytes::from_static(b"v1")),
                    (Bytes::from_static(b"1"), Bytes::from_static(b"")),
                    (Bytes::from_static(b"1"), Bytes::from_static(b"v2")),
                    (Bytes::from_static(b"2"), Bytes::from_static(b"v3")),
                    (Bytes::from_static(b"2"), Bytes::from_static(b"")),
                    (Bytes::from_static(b"2"), Bytes::from_static(b"v2")),
                ],
            );
        } else {
            check_iter_result_by_key(
                &mut iter,
                vec![
                    (Bytes::from_static(b"0"), Bytes::from_static(b"v3")),
                    (Bytes::from_static(b"1"), Bytes::from_static(b"")),
                    (Bytes::from_static(b"2"), Bytes::from_static(b"v3")),
                ],
            );
        }
        storage.force_full_compaction().unwrap();
        assert!(storage.state.read().l0_sstables.is_empty());
        let mut iter = construct_merge_iterator_over_storage(&storage.state.read());
        if TS_ENABLED {
            check_iter_result_by_key(
                &mut iter,
                vec![
                    (Bytes::from_static(b"0"), Bytes::from_static(b"v3")),
                    (Bytes::from_static(b"0"), Bytes::from_static(b"")),
                    (Bytes::from_static(b"0"), Bytes::from_static(b"v2")),
                    (Bytes::from_static(b"0"), Bytes::from_static(b"v1")),
                    (Bytes::from_static(b"1"), Bytes::from_static(b"")),
                    (Bytes::from_static(b"1"), Bytes::from_static(b"v2")),
                    (Bytes::from_static(b"2"), Bytes::from_static(b"v3")),
                    (Bytes::from_static(b"2"), Bytes::from_static(b"")),
                    (Bytes::from_static(b"2"), Bytes::from_static(b"v2")),
                ],
            );
        } else {
            check_iter_result_by_key(
                &mut iter,
                vec![
                    (Bytes::from_static(b"0"), Bytes::from_static(b"v3")),
                    (Bytes::from_static(b"2"), Bytes::from_static(b"v3")),
                ],
            );
        }
    }
    
    fn generate_concat_sst(
        start_key: usize,
        end_key: usize,
        dir: impl AsRef<Path>,
        id: usize,
    ) -> SsTable {
        let mut builder = SsTableBuilder::new(128);
        for idx in start_key..end_key {
            let key = format!("{:05}", idx);
            builder.add(
                KeySlice::for_testing_from_slice_no_ts(key.as_bytes()),
                b"test",
            );
        }
        let path = dir.as_ref().join(format!("{id}.sst"));
        builder.build_for_test(path).unwrap()
    }
    
    #[test]
    fn test_task2_concat_iterator() {
        let dir = tempdir().unwrap();
        let mut sstables = Vec::new();
        for i in 1..=10 {
            sstables.push(Arc::new(generate_concat_sst(
                i * 10,
                (i + 1) * 10,
                dir.path(),
                i,
            )));
        }
        for key in 0..120 {
            let iter = SstConcatIterator::create_and_seek_to_key(
                sstables.clone(),
                KeySlice::for_testing_from_slice_no_ts(format!("{:05}", key).as_bytes()),
            )
            .unwrap();
            if key < 10 {
                assert!(iter.is_valid());
                assert_eq!(iter.key().for_testing_key_ref(), b"00010");
            } else if key >= 110 {
                assert!(!iter.is_valid());
            } else {
                assert!(iter.is_valid());
                assert_eq!(
                    iter.key().for_testing_key_ref(),
                    format!("{:05}", key).as_bytes()
                );
            }
        }
        let iter = SstConcatIterator::create_and_seek_to_first(sstables.clone()).unwrap();
        assert!(iter.is_valid());
        assert_eq!(iter.key().for_testing_key_ref(), b"00010");
    }
    
    #[test]
    fn test_task3_integration() {
        let dir = tempdir().unwrap();
        let storage =
            Arc::new(LsmStorageInner::open(&dir, LsmStorageOptions::default()).unwrap());
        storage.put(b"0", b"2333333").unwrap();
        storage.put(b"00", b"2333333").unwrap();
        storage.put(b"4", b"23").unwrap();
        sync(&storage);
    
        storage.delete(b"4").unwrap();
        sync(&storage);
    
        storage.force_full_compaction().unwrap();
        assert!(storage.state.read().l0_sstables.is_empty());
        assert!(!storage.state.read().levels[0].1.is_empty());
    
        storage.put(b"1", b"233").unwrap();
        storage.put(b"2", b"2333").unwrap();
        sync(&storage);
    
        storage.put(b"00", b"2333").unwrap();
        storage.put(b"3", b"23333").unwrap();
        storage.delete(b"1").unwrap();
        sync(&storage);
        storage.force_full_compaction().unwrap();
    
        assert!(storage.state.read().l0_sstables.is_empty());
        assert!(!storage.state.read().levels[0].1.is_empty());
    
        check_lsm_iter_result_by_key(
            &mut storage.scan(Bound::Unbounded, Bound::Unbounded).unwrap(),
            vec![
                (Bytes::from("0"), Bytes::from("2333333")),
                (Bytes::from("00"), Bytes::from("2333")),
                (Bytes::from("2"), Bytes::from("2333")),
                (Bytes::from("3"), Bytes::from("23333")),
            ],
        );
    
        assert_eq!(
            storage.get(b"0").unwrap(),
            Some(Bytes::from_static(b"2333333"))
        );
        assert_eq!(
            storage.get(b"00").unwrap(),
            Some(Bytes::from_static(b"2333"))
        );
        assert_eq!(
            storage.get(b"2").unwrap(),
            Some(Bytes::from_static(b"2333"))
        );
        assert_eq!(
            storage.get(b"3").unwrap(),
            Some(Bytes::from_static(b"23333"))
        );
        assert_eq!(storage.get(b"4").unwrap(), None);
        assert_eq!(storage.get(b"--").unwrap(), None);
        assert_eq!(storage.get(b"555").unwrap(), None);
    }
    
}
