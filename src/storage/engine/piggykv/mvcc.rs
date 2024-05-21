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
    pub(crate) commit_lock: Mutex<()>,

    pub(crate) ts: Arc<Mutex<(u64, Watermark)>>,
    pub(crate) committed_txns: Arc<Mutex<BTreeMap<u64, CommittedTxnData>>>,
}

impl LsmMvccInner {
    pub fn new(initial_ts: u64) -> Self {
        Self {
            write_lock: Mutex::new(()),
            commit_lock: Mutex::new(()),

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
        let mut ts = self.ts.lock();
        let read_ts = ts.0;
        ts.1.add_reader(read_ts);

        Arc::new(Transaction {
            inner,
            read_ts,
            local_storage: Arc::new(SkipMap::new()),
            committed: Arc::new(AtomicBool::new(false)),
            key_hashes: if serializable {
                Some(Mutex::new((HashSet::new(), HashSet::new())))
            } else {
                None
            },
            serialize: Arc::new(AtomicBool::new(serializable)),
        })
    }
}

#[cfg(test)]
mod tests {

    use std::{ops::Bound, sync::Arc, time::Duration};

    use bytes::Bytes;
    use tempfile::tempdir;

    use crate::storage::engine::piggykv::{
        compact::CompactionOptions,
        debug::{
            check_iter_result_by_key_and_ts, check_lsm_iter_result_by_key, dump_files_in_dir,
            generate_sst_with_ts,
        },
        key::KeySlice,
        lsm_storage::LsmStorageOptions,
        table::{FileObject, SsTable, SsTableBuilder, SsTableIterator},
        PiggyKV,
    };

    #[test]
    fn test_sst_build_multi_version_simple() {
        let mut builder = SsTableBuilder::new(16);
        builder.add(
            KeySlice::for_testing_from_slice_with_ts(b"233", 233),
            b"233333",
        );
        builder.add(
            KeySlice::for_testing_from_slice_with_ts(b"233", 0),
            b"2333333",
        );
        let dir = tempdir().unwrap();
        builder.build_for_test(dir.path().join("1.sst")).unwrap();
    }

    fn generate_test_data() -> Vec<((Bytes, u64), Bytes)> {
        (0..100)
            .map(|id| {
                (
                    (Bytes::from(format!("key{:05}", id / 5)), 5 - (id % 5)),
                    Bytes::from(format!("value{:05}", id)),
                )
            })
            .collect()
    }

    #[test]
    fn test_sst_build_multi_version_hard() {
        let dir = tempdir().unwrap();
        let data = generate_test_data();
        generate_sst_with_ts(1, dir.path().join("1.sst"), data.clone(), None);
        let sst = Arc::new(
            SsTable::open(
                1,
                None,
                FileObject::open(&dir.path().join("1.sst")).unwrap(),
            )
            .unwrap(),
        );
        check_iter_result_by_key_and_ts(
            &mut SsTableIterator::create_and_seek_to_first(sst).unwrap(),
            data,
        );
    }

    #[test]
    fn test_task2_memtable_mvcc() {
        let dir = tempdir().unwrap();
        let mut options = LsmStorageOptions::default();
        let storage = PiggyKV::open(&dir, options.clone()).unwrap();
        storage.put(b"a", b"1").unwrap();
        storage.put(b"b", b"1").unwrap();
        let snapshot1 = storage.new_txn().unwrap();
        storage.put(b"a", b"2").unwrap();
        let snapshot2 = storage.new_txn().unwrap();
        storage.delete(b"b").unwrap();
        storage.put(b"c", b"1").unwrap();
        let snapshot3 = storage.new_txn().unwrap();
        assert_eq!(snapshot1.get(b"a").unwrap(), Some(Bytes::from_static(b"1")));
        assert_eq!(snapshot1.get(b"b").unwrap(), Some(Bytes::from_static(b"1")));
        assert_eq!(snapshot1.get(b"c").unwrap(), None);
        check_lsm_iter_result_by_key(
            &mut snapshot1.scan(Bound::Unbounded, Bound::Unbounded).unwrap(),
            vec![
                (Bytes::from("a"), Bytes::from("1")),
                (Bytes::from("b"), Bytes::from("1")),
            ],
        );
        assert_eq!(snapshot2.get(b"a").unwrap(), Some(Bytes::from_static(b"2")));
        assert_eq!(snapshot2.get(b"b").unwrap(), Some(Bytes::from_static(b"1")));
        assert_eq!(snapshot2.get(b"c").unwrap(), None);
        check_lsm_iter_result_by_key(
            &mut snapshot2.scan(Bound::Unbounded, Bound::Unbounded).unwrap(),
            vec![
                (Bytes::from("a"), Bytes::from("2")),
                (Bytes::from("b"), Bytes::from("1")),
            ],
        );
        assert_eq!(snapshot3.get(b"a").unwrap(), Some(Bytes::from_static(b"2")));
        assert_eq!(snapshot3.get(b"b").unwrap(), None);
        assert_eq!(snapshot3.get(b"c").unwrap(), Some(Bytes::from_static(b"1")));
        check_lsm_iter_result_by_key(
            &mut snapshot3.scan(Bound::Unbounded, Bound::Unbounded).unwrap(),
            vec![
                (Bytes::from("a"), Bytes::from("2")),
                (Bytes::from("c"), Bytes::from("1")),
            ],
        );
        storage
            .inner
            .force_freeze_memtable(&storage.inner.state_lock.lock())
            .unwrap();
        storage.put(b"a", b"3").unwrap();
        storage.put(b"b", b"3").unwrap();
        let snapshot4 = storage.new_txn().unwrap();
        storage.put(b"a", b"4").unwrap();
        let snapshot5 = storage.new_txn().unwrap();
        storage.delete(b"b").unwrap();
        storage.put(b"c", b"5").unwrap();
        let snapshot6 = storage.new_txn().unwrap();
        assert_eq!(snapshot1.get(b"a").unwrap(), Some(Bytes::from_static(b"1")));
        assert_eq!(snapshot1.get(b"b").unwrap(), Some(Bytes::from_static(b"1")));
        assert_eq!(snapshot1.get(b"c").unwrap(), None);
        check_lsm_iter_result_by_key(
            &mut snapshot1.scan(Bound::Unbounded, Bound::Unbounded).unwrap(),
            vec![
                (Bytes::from("a"), Bytes::from("1")),
                (Bytes::from("b"), Bytes::from("1")),
            ],
        );
        assert_eq!(snapshot2.get(b"a").unwrap(), Some(Bytes::from_static(b"2")));
        assert_eq!(snapshot2.get(b"b").unwrap(), Some(Bytes::from_static(b"1")));
        assert_eq!(snapshot2.get(b"c").unwrap(), None);
        check_lsm_iter_result_by_key(
            &mut snapshot2.scan(Bound::Unbounded, Bound::Unbounded).unwrap(),
            vec![
                (Bytes::from("a"), Bytes::from("2")),
                (Bytes::from("b"), Bytes::from("1")),
            ],
        );
        assert_eq!(snapshot3.get(b"a").unwrap(), Some(Bytes::from_static(b"2")));
        assert_eq!(snapshot3.get(b"b").unwrap(), None);
        assert_eq!(snapshot3.get(b"c").unwrap(), Some(Bytes::from_static(b"1")));
        check_lsm_iter_result_by_key(
            &mut snapshot3.scan(Bound::Unbounded, Bound::Unbounded).unwrap(),
            vec![
                (Bytes::from("a"), Bytes::from("2")),
                (Bytes::from("c"), Bytes::from("1")),
            ],
        );
        assert_eq!(snapshot4.get(b"a").unwrap(), Some(Bytes::from_static(b"3")));
        assert_eq!(snapshot4.get(b"b").unwrap(), Some(Bytes::from_static(b"3")));
        assert_eq!(snapshot4.get(b"c").unwrap(), Some(Bytes::from_static(b"1")));
        check_lsm_iter_result_by_key(
            &mut snapshot4.scan(Bound::Unbounded, Bound::Unbounded).unwrap(),
            vec![
                (Bytes::from("a"), Bytes::from("3")),
                (Bytes::from("b"), Bytes::from("3")),
                (Bytes::from("c"), Bytes::from("1")),
            ],
        );
        assert_eq!(snapshot5.get(b"a").unwrap(), Some(Bytes::from_static(b"4")));
        assert_eq!(snapshot5.get(b"b").unwrap(), Some(Bytes::from_static(b"3")));
        assert_eq!(snapshot5.get(b"c").unwrap(), Some(Bytes::from_static(b"1")));
        check_lsm_iter_result_by_key(
            &mut snapshot5.scan(Bound::Unbounded, Bound::Unbounded).unwrap(),
            vec![
                (Bytes::from("a"), Bytes::from("4")),
                (Bytes::from("b"), Bytes::from("3")),
                (Bytes::from("c"), Bytes::from("1")),
            ],
        );
        assert_eq!(snapshot6.get(b"a").unwrap(), Some(Bytes::from_static(b"4")));
        assert_eq!(snapshot6.get(b"b").unwrap(), None);
        assert_eq!(snapshot6.get(b"c").unwrap(), Some(Bytes::from_static(b"5")));
        check_lsm_iter_result_by_key(
            &mut snapshot6.scan(Bound::Unbounded, Bound::Unbounded).unwrap(),
            vec![
                (Bytes::from("a"), Bytes::from("4")),
                (Bytes::from("c"), Bytes::from("5")),
            ],
        );
    }

    #[test]
    fn test_task2_lsm_iterator_mvcc() {
        let dir = tempdir().unwrap();
        let options = LsmStorageOptions::default();
        let storage = PiggyKV::open(&dir, options.clone()).unwrap();
        storage.put(b"a", b"1").unwrap();
        storage.put(b"b", b"1").unwrap();
        let snapshot1 = storage.new_txn().unwrap();
        storage.put(b"a", b"2").unwrap();
        let snapshot2 = storage.new_txn().unwrap();
        storage.delete(b"b").unwrap();
        storage.put(b"c", b"1").unwrap();
        let snapshot3 = storage.new_txn().unwrap();
        storage.force_flush().unwrap();
        assert_eq!(snapshot1.get(b"a").unwrap(), Some(Bytes::from_static(b"1")));
        assert_eq!(snapshot1.get(b"b").unwrap(), Some(Bytes::from_static(b"1")));
        assert_eq!(snapshot1.get(b"c").unwrap(), None);
        check_lsm_iter_result_by_key(
            &mut snapshot1.scan(Bound::Unbounded, Bound::Unbounded).unwrap(),
            vec![
                (Bytes::from("a"), Bytes::from("1")),
                (Bytes::from("b"), Bytes::from("1")),
            ],
        );
        assert_eq!(snapshot2.get(b"a").unwrap(), Some(Bytes::from_static(b"2")));
        assert_eq!(snapshot2.get(b"b").unwrap(), Some(Bytes::from_static(b"1")));
        assert_eq!(snapshot2.get(b"c").unwrap(), None);
        check_lsm_iter_result_by_key(
            &mut snapshot2.scan(Bound::Unbounded, Bound::Unbounded).unwrap(),
            vec![
                (Bytes::from("a"), Bytes::from("2")),
                (Bytes::from("b"), Bytes::from("1")),
            ],
        );
        assert_eq!(snapshot3.get(b"a").unwrap(), Some(Bytes::from_static(b"2")));
        assert_eq!(snapshot3.get(b"b").unwrap(), None);
        assert_eq!(snapshot3.get(b"c").unwrap(), Some(Bytes::from_static(b"1")));
        check_lsm_iter_result_by_key(
            &mut snapshot3.scan(Bound::Unbounded, Bound::Unbounded).unwrap(),
            vec![
                (Bytes::from("a"), Bytes::from("2")),
                (Bytes::from("c"), Bytes::from("1")),
            ],
        );
        storage.put(b"a", b"3").unwrap();
        storage.put(b"b", b"3").unwrap();
        let snapshot4 = storage.new_txn().unwrap();
        storage.put(b"a", b"4").unwrap();
        let snapshot5 = storage.new_txn().unwrap();
        storage.delete(b"b").unwrap();
        storage.put(b"c", b"5").unwrap();
        let snapshot6 = storage.new_txn().unwrap();
        storage.force_flush().unwrap();
        assert_eq!(snapshot1.get(b"a").unwrap(), Some(Bytes::from_static(b"1")));
        assert_eq!(snapshot1.get(b"b").unwrap(), Some(Bytes::from_static(b"1")));
        assert_eq!(snapshot1.get(b"c").unwrap(), None);
        check_lsm_iter_result_by_key(
            &mut snapshot1.scan(Bound::Unbounded, Bound::Unbounded).unwrap(),
            vec![
                (Bytes::from("a"), Bytes::from("1")),
                (Bytes::from("b"), Bytes::from("1")),
            ],
        );
        assert_eq!(snapshot2.get(b"a").unwrap(), Some(Bytes::from_static(b"2")));
        assert_eq!(snapshot2.get(b"b").unwrap(), Some(Bytes::from_static(b"1")));
        assert_eq!(snapshot2.get(b"c").unwrap(), None);
        check_lsm_iter_result_by_key(
            &mut snapshot2.scan(Bound::Unbounded, Bound::Unbounded).unwrap(),
            vec![
                (Bytes::from("a"), Bytes::from("2")),
                (Bytes::from("b"), Bytes::from("1")),
            ],
        );
        assert_eq!(snapshot3.get(b"a").unwrap(), Some(Bytes::from_static(b"2")));
        assert_eq!(snapshot3.get(b"b").unwrap(), None);
        assert_eq!(snapshot3.get(b"c").unwrap(), Some(Bytes::from_static(b"1")));
        check_lsm_iter_result_by_key(
            &mut snapshot3.scan(Bound::Unbounded, Bound::Unbounded).unwrap(),
            vec![
                (Bytes::from("a"), Bytes::from("2")),
                (Bytes::from("c"), Bytes::from("1")),
            ],
        );
        assert_eq!(snapshot4.get(b"a").unwrap(), Some(Bytes::from_static(b"3")));
        assert_eq!(snapshot4.get(b"b").unwrap(), Some(Bytes::from_static(b"3")));
        assert_eq!(snapshot4.get(b"c").unwrap(), Some(Bytes::from_static(b"1")));
        check_lsm_iter_result_by_key(
            &mut snapshot4.scan(Bound::Unbounded, Bound::Unbounded).unwrap(),
            vec![
                (Bytes::from("a"), Bytes::from("3")),
                (Bytes::from("b"), Bytes::from("3")),
                (Bytes::from("c"), Bytes::from("1")),
            ],
        );
        assert_eq!(snapshot5.get(b"a").unwrap(), Some(Bytes::from_static(b"4")));
        assert_eq!(snapshot5.get(b"b").unwrap(), Some(Bytes::from_static(b"3")));
        assert_eq!(snapshot5.get(b"c").unwrap(), Some(Bytes::from_static(b"1")));
        check_lsm_iter_result_by_key(
            &mut snapshot5.scan(Bound::Unbounded, Bound::Unbounded).unwrap(),
            vec![
                (Bytes::from("a"), Bytes::from("4")),
                (Bytes::from("b"), Bytes::from("3")),
                (Bytes::from("c"), Bytes::from("1")),
            ],
        );
        assert_eq!(snapshot6.get(b"a").unwrap(), Some(Bytes::from_static(b"4")));
        assert_eq!(snapshot6.get(b"b").unwrap(), None);
        assert_eq!(snapshot6.get(b"c").unwrap(), Some(Bytes::from_static(b"5")));
        check_lsm_iter_result_by_key(
            &mut snapshot6.scan(Bound::Unbounded, Bound::Unbounded).unwrap(),
            vec![
                (Bytes::from("a"), Bytes::from("4")),
                (Bytes::from("c"), Bytes::from("5")),
            ],
        );
        check_lsm_iter_result_by_key(
            &mut snapshot6
                .scan(Bound::Included(b"a"), Bound::Included(b"a"))
                .unwrap(),
            vec![(Bytes::from("a"), Bytes::from("4"))],
        );
        check_lsm_iter_result_by_key(
            &mut snapshot6
                .scan(Bound::Excluded(b"a"), Bound::Excluded(b"c"))
                .unwrap(),
            vec![],
        );
    }

    #[test]
    fn test_task3_sst_ts() {
        let mut builder = SsTableBuilder::new(16);
        builder.add(KeySlice::for_testing_from_slice_with_ts(b"11", 1), b"11");
        builder.add(KeySlice::for_testing_from_slice_with_ts(b"22", 2), b"22");
        builder.add(KeySlice::for_testing_from_slice_with_ts(b"33", 3), b"11");
        builder.add(KeySlice::for_testing_from_slice_with_ts(b"44", 4), b"22");
        builder.add(KeySlice::for_testing_from_slice_with_ts(b"55", 5), b"11");
        builder.add(KeySlice::for_testing_from_slice_with_ts(b"66", 6), b"22");
        let dir = tempdir().unwrap();
        let sst = builder.build_for_test(dir.path().join("1.sst")).unwrap();
        assert_eq!(sst.max_ts(), 6);
    }




}
