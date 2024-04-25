use std::cmp::{self};
use std::collections::binary_heap::PeekMut;
use std::collections::BinaryHeap;

use crate::errors::Result;
use crate::storage::engine::piggykv::key::KeySlice;


use super::StorageIterator;

struct HeapWrapper<I: StorageIterator>(pub usize, pub Box<I>);

impl<I: StorageIterator> PartialEq for HeapWrapper<I> {
    fn eq(&self, other: &Self) -> bool {
        self.partial_cmp(other).unwrap() == cmp::Ordering::Equal
    }
}

impl<I: StorageIterator> Eq for HeapWrapper<I> {}

impl<I: StorageIterator> PartialOrd for HeapWrapper<I> {
    #[allow(clippy::non_canonical_partial_ord_impl)]
    fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
        match self.1.key().cmp(&other.1.key()) {
            cmp::Ordering::Greater => Some(cmp::Ordering::Greater),
            cmp::Ordering::Less => Some(cmp::Ordering::Less),
            cmp::Ordering::Equal => self.0.partial_cmp(&other.0),
        }
        .map(|x| x.reverse())
    }
}

impl<I: StorageIterator> Ord for HeapWrapper<I> {
    fn cmp(&self, other: &Self) -> cmp::Ordering {
        self.partial_cmp(other).unwrap()
    }
}

/// Merge multiple iterators of the same type. If the same key occurs multiple times in some
/// iterators, perfer the one with smaller index.
pub struct MergeIterator<I: StorageIterator> {
    iters: BinaryHeap<HeapWrapper<I>>,
    current: Option<HeapWrapper<I>>,
}

impl<I: StorageIterator> MergeIterator<I> {
    pub fn create(iters: Vec<Box<I>>) -> Self {
        if iters.is_empty() {
            return Self {
                iters: BinaryHeap::new(),
                current: None,
            };
        }

        let mut heap = BinaryHeap::new();

        if iters.iter().all(|x| !x.is_valid()) {
            // All invalid, select the last one as the current.
            let mut iters = iters;
            return Self {
                iters: heap,
                current: Some(HeapWrapper(0, iters.pop().unwrap())),
            };
        }

        for (idx, iter) in iters.into_iter().enumerate() {
            if iter.is_valid() {
                heap.push(HeapWrapper(idx, iter));
            }
        }

        let current = heap.pop().unwrap();
        Self {
            iters: heap,
            current: Some(current),
        }
    }
}

impl<I: 'static + for<'a> StorageIterator<KeyType<'a> = KeySlice<'a>>> StorageIterator
    for MergeIterator<I>
{
    type KeyType<'a> = KeySlice<'a>;

    fn key(&self) -> KeySlice {
        self.current.as_ref().unwrap().1.key()
    }

    fn value(&self) -> &[u8] {
        self.current.as_ref().unwrap().1.value()
    }

    fn is_valid(&self) -> bool {
        self.current
            .as_ref()
            .map(|x| x.1.is_valid())
            .unwrap_or(false)
    }

    fn _next(&mut self) -> Result<()> {
        let current = self.current.as_mut().unwrap();
        // Pop the item out of the heap if they have the same value.
        while let Some(mut inner_iter) = self.iters.peek_mut() {
            debug_assert!(
                inner_iter.1.key() >= current.1.key(),
                "heap invariant violated"
            );
            if inner_iter.1.key() == current.1.key() {
                // Case 1: an error occurred when calling `next`.
                if let e @ Err(_) = inner_iter.1._next() {
                    PeekMut::pop(inner_iter);
                    return e;
                }

                // Case 2: iter is no longer valid.
                if !inner_iter.1.is_valid() {
                    PeekMut::pop(inner_iter);
                }
            } else {
                break;
            }
        }

        current.1._next()?;

        // If the current iterator is invalid, pop it out of the heap and select the next one.
        if !current.1.is_valid() {
            if let Some(iter) = self.iters.pop() {
                *current = iter;
            }
            return Ok(());
        }

        // Otherwise, compare with heap top and swap if necessary.
        if let Some(mut inner_iter) = self.iters.peek_mut() {
            if *current < *inner_iter {
                std::mem::swap(&mut *inner_iter, current);
            }
        }

        Ok(())
    }

    fn num_active_iterators(&self) -> usize {
        self.iters
            .iter()
            .map(|x| x.1.num_active_iterators())
            .sum::<usize>()
            + self
                .current
                .as_ref()
                .map(|x| x.1.num_active_iterators())
                .unwrap_or(0)
    }
}


#[cfg(test)]
mod test {
    use std::{ops::Bound, sync::Arc, time::Duration};

    use bytes::Bytes;

    use crate::storage::engine::piggykv::{debug::{check_iter_result_by_key, check_lsm_iter_result_by_key, generate_sst, sync, MockIterator}, iterators::{two_merge_iterator::TwoMergeIterator, StorageIterator}, key::{KeySlice, KeyVec}, lsm_storage::{LsmStorageInner, LsmStorageOptions}, PiggyKV};
    use tempfile::{tempdir, TempDir};
    use super::*;

    #[test]
    fn test_task1_merge_1() {
        let i1 = MockIterator::new(vec![
            (Bytes::from("a"), Bytes::from("1.1")),
            (Bytes::from("b"), Bytes::from("2.1")),
            (Bytes::from("c"), Bytes::from("3.1")),
        ]);
        let i2 = MockIterator::new(vec![
            (Bytes::from("a"), Bytes::from("1.2")),
            (Bytes::from("b"), Bytes::from("2.2")),
            (Bytes::from("c"), Bytes::from("3.2")),
            (Bytes::from("d"), Bytes::from("4.2")),
        ]);
        let mut iter = TwoMergeIterator::create(i1, i2).unwrap();
        check_iter_result_by_key(
            &mut iter,
            vec![
                (Bytes::from("a"), Bytes::from("1.1")),
                (Bytes::from("b"), Bytes::from("2.1")),
                (Bytes::from("c"), Bytes::from("3.1")),
                (Bytes::from("d"), Bytes::from("4.2")),
            ],
        )
    }
    
    #[test]
    fn test_task1_merge_2() {
        let i2 = MockIterator::new(vec![
            (Bytes::from("a"), Bytes::from("1.1")),
            (Bytes::from("b"), Bytes::from("2.1")),
            (Bytes::from("c"), Bytes::from("3.1")),
        ]);
        let i1 = MockIterator::new(vec![
            (Bytes::from("a"), Bytes::from("1.2")),
            (Bytes::from("b"), Bytes::from("2.2")),
            (Bytes::from("c"), Bytes::from("3.2")),
            (Bytes::from("d"), Bytes::from("4.2")),
        ]);
        let mut iter = TwoMergeIterator::create(i1, i2).unwrap();
        check_iter_result_by_key(
            &mut iter,
            vec![
                (Bytes::from("a"), Bytes::from("1.2")),
                (Bytes::from("b"), Bytes::from("2.2")),
                (Bytes::from("c"), Bytes::from("3.2")),
                (Bytes::from("d"), Bytes::from("4.2")),
            ],
        )
    }
    
    #[test]
    fn test_task1_merge_3() {
        let i2 = MockIterator::new(vec![
            (Bytes::from("a"), Bytes::from("1.1")),
            (Bytes::from("b"), Bytes::from("2.1")),
            (Bytes::from("c"), Bytes::from("3.1")),
        ]);
        let i1 = MockIterator::new(vec![
            (Bytes::from("b"), Bytes::from("2.2")),
            (Bytes::from("c"), Bytes::from("3.2")),
            (Bytes::from("d"), Bytes::from("4.2")),
        ]);
        let mut iter = TwoMergeIterator::create(i1, i2).unwrap();
        check_iter_result_by_key(
            &mut iter,
            vec![
                (Bytes::from("a"), Bytes::from("1.1")),
                (Bytes::from("b"), Bytes::from("2.2")),
                (Bytes::from("c"), Bytes::from("3.2")),
                (Bytes::from("d"), Bytes::from("4.2")),
            ],
        )
    }
    
    #[test]
    fn test_task1_merge_4() {
        let i2 = MockIterator::new(vec![]);
        let i1 = MockIterator::new(vec![
            (Bytes::from("b"), Bytes::from("2.2")),
            (Bytes::from("c"), Bytes::from("3.2")),
            (Bytes::from("d"), Bytes::from("4.2")),
        ]);
        let mut iter = TwoMergeIterator::create(i1, i2).unwrap();
        check_iter_result_by_key(
            &mut iter,
            vec![
                (Bytes::from("b"), Bytes::from("2.2")),
                (Bytes::from("c"), Bytes::from("3.2")),
                (Bytes::from("d"), Bytes::from("4.2")),
            ],
        );
        let i1 = MockIterator::new(vec![]);
        let i2 = MockIterator::new(vec![
            (Bytes::from("b"), Bytes::from("2.2")),
            (Bytes::from("c"), Bytes::from("3.2")),
            (Bytes::from("d"), Bytes::from("4.2")),
        ]);
        let mut iter = TwoMergeIterator::create(i1, i2).unwrap();
        check_iter_result_by_key(
            &mut iter,
            vec![
                (Bytes::from("b"), Bytes::from("2.2")),
                (Bytes::from("c"), Bytes::from("3.2")),
                (Bytes::from("d"), Bytes::from("4.2")),
            ],
        );
    }
    
    #[test]
    fn test_task1_merge_5() {
        let i2 = MockIterator::new(vec![]);
        let i1 = MockIterator::new(vec![]);
        let mut iter = TwoMergeIterator::create(i1, i2).unwrap();
        check_iter_result_by_key(&mut iter, vec![])
    }
    
    #[test]
    fn test_task2_storage_scan() {
        let dir = tempdir().unwrap();
        let storage =
            Arc::new(LsmStorageInner::open(&dir, LsmStorageOptions::default()).unwrap());
        storage.put(b"1", b"233").unwrap();
        storage.put(b"2", b"2333").unwrap();
        storage.put(b"00", b"2333").unwrap();
        storage
            .force_freeze_memtable(&storage.state_lock.lock())
            .unwrap();
        storage.put(b"3", b"23333").unwrap();
        storage.delete(b"1").unwrap();
        let sst1 = generate_sst(
            10,
            dir.path().join("10.sst"),
            vec![
                (Bytes::from_static(b"0"), Bytes::from_static(b"2333333")),
                (Bytes::from_static(b"00"), Bytes::from_static(b"2333333")),
                (Bytes::from_static(b"4"), Bytes::from_static(b"23")),
            ],
            Some(storage.block_cache.clone()),
        );
        let sst2 = generate_sst(
            11,
            dir.path().join("11.sst"),
            vec![(Bytes::from_static(b"4"), Bytes::from_static(b""))],
            Some(storage.block_cache.clone()),
        );
        {
            let mut state = storage.state.write();
            let mut snapshot = state.as_ref().clone();
            snapshot.l0_sstables.push(sst2.sst_id()); // this is the latest SST
            snapshot.l0_sstables.push(sst1.sst_id());
            snapshot.sstables.insert(sst2.sst_id(), sst2.into());
            snapshot.sstables.insert(sst1.sst_id(), sst1.into());
            *state = snapshot.into();
        }
        check_lsm_iter_result_by_key(
            &mut storage.scan(Bound::Unbounded, Bound::Unbounded).unwrap(),
            vec![
                (Bytes::from("0"), Bytes::from("2333333")),
                (Bytes::from("00"), Bytes::from("2333")),
                (Bytes::from("2"), Bytes::from("2333")),
                (Bytes::from("3"), Bytes::from("23333")),
            ],
        );
        check_lsm_iter_result_by_key(
            &mut storage
                .scan(Bound::Included(b"1"), Bound::Included(b"2"))
                .unwrap(),
            vec![(Bytes::from("2"), Bytes::from("2333"))],
        );
        check_lsm_iter_result_by_key(
            &mut storage
                .scan(Bound::Excluded(b"1"), Bound::Excluded(b"3"))
                .unwrap(),
            vec![(Bytes::from("2"), Bytes::from("2333"))],
        );
        check_lsm_iter_result_by_key(
            &mut storage
                .scan(Bound::Included(b"0"), Bound::Included(b"1"))
                .unwrap(),
            vec![
                (Bytes::from_static(b"0"), Bytes::from_static(b"2333333")),
                (Bytes::from("00"), Bytes::from("2333")),
            ],
        );
        check_lsm_iter_result_by_key(
            &mut storage
                .scan(Bound::Excluded(b"0"), Bound::Included(b"1"))
                .unwrap(),
            vec![(Bytes::from("00"), Bytes::from("2333"))],
        );
    }
    
    #[test]
    fn test_task3_storage_get() {
        let dir = tempdir().unwrap();
        let storage =
            Arc::new(LsmStorageInner::open(&dir, LsmStorageOptions::default()).unwrap());
        storage.put(b"1", b"233").unwrap();
        storage.put(b"2", b"2333").unwrap();
        storage.put(b"00", b"2333").unwrap();
        storage
            .force_freeze_memtable(&storage.state_lock.lock())
            .unwrap();
        storage.put(b"3", b"23333").unwrap();
        storage.delete(b"1").unwrap();
        let sst1 = generate_sst(
            10,
            dir.path().join("10.sst"),
            vec![
                (Bytes::from_static(b"0"), Bytes::from_static(b"2333333")),
                (Bytes::from_static(b"00"), Bytes::from_static(b"2333333")),
                (Bytes::from_static(b"4"), Bytes::from_static(b"23")),
            ],
            Some(storage.block_cache.clone()),
        );
        let sst2 = generate_sst(
            11,
            dir.path().join("11.sst"),
            vec![(Bytes::from_static(b"4"), Bytes::from_static(b""))],
            Some(storage.block_cache.clone()),
        );
        {
            let mut state = storage.state.write();
            let mut snapshot = state.as_ref().clone();
            snapshot.l0_sstables.push(sst2.sst_id()); // this is the latest SST
            snapshot.l0_sstables.push(sst1.sst_id());
            snapshot.sstables.insert(sst2.sst_id(), sst2.into());
            snapshot.sstables.insert(sst1.sst_id(), sst1.into());
            *state = snapshot.into();
        }
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
    #[test]
fn test_task1_storage_scan() {
    let dir = tempdir().unwrap();
    let storage =
        Arc::new(LsmStorageInner::open(&dir, LsmStorageOptions::default()).unwrap());
    storage.put(b"0", b"2333333").unwrap();
    storage.put(b"00", b"2333333").unwrap();
    storage.put(b"4", b"23").unwrap();
    sync(&storage);

    storage.delete(b"4").unwrap();
    sync(&storage);

    storage.put(b"1", b"233").unwrap();
    storage.put(b"2", b"2333").unwrap();
    storage
        .force_freeze_memtable(&storage.state_lock.lock())
        .unwrap();
    storage.put(b"00", b"2333").unwrap();
    storage
        .force_freeze_memtable(&storage.state_lock.lock())
        .unwrap();
    storage.put(b"3", b"23333").unwrap();
    storage.delete(b"1").unwrap();

    {
        let state = storage.state.read();
        assert_eq!(state.l0_sstables.len(), 2);
        assert_eq!(state.imm_memtables.len(), 2);
    }

    check_lsm_iter_result_by_key(
        &mut storage.scan(Bound::Unbounded, Bound::Unbounded).unwrap(),
        vec![
            (Bytes::from("0"), Bytes::from("2333333")),
            (Bytes::from("00"), Bytes::from("2333")),
            (Bytes::from("2"), Bytes::from("2333")),
            (Bytes::from("3"), Bytes::from("23333")),
        ],
    );
    check_lsm_iter_result_by_key(
        &mut storage
            .scan(Bound::Included(b"1"), Bound::Included(b"2"))
            .unwrap(),
        vec![(Bytes::from("2"), Bytes::from("2333"))],
    );
    check_lsm_iter_result_by_key(
        &mut storage
            .scan(Bound::Excluded(b"1"), Bound::Excluded(b"3"))
            .unwrap(),
        vec![(Bytes::from("2"), Bytes::from("2333"))],
    );
}

#[test]
fn test_task1_storage_get() {
    let dir = tempdir().unwrap();
    let storage =
        Arc::new(LsmStorageInner::open(&dir, LsmStorageOptions::default()).unwrap());
    storage.put(b"0", b"2333333").unwrap();
    storage.put(b"00", b"2333333").unwrap();
    storage.put(b"4", b"23").unwrap();
    sync(&storage);

    storage.delete(b"4").unwrap();
    sync(&storage);

    storage.put(b"1", b"233").unwrap();
    storage.put(b"2", b"2333").unwrap();
    storage
        .force_freeze_memtable(&storage.state_lock.lock())
        .unwrap();
    storage.put(b"00", b"2333").unwrap();
    storage
        .force_freeze_memtable(&storage.state_lock.lock())
        .unwrap();
    storage.put(b"3", b"23333").unwrap();
    storage.delete(b"1").unwrap();

    {
        let state = storage.state.read();
        assert_eq!(state.l0_sstables.len(), 2);
        assert_eq!(state.imm_memtables.len(), 2);
    }

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

#[test]
fn test_task2_auto_flush() {
    let dir = tempdir().unwrap();
    let storage = PiggyKV::open(&dir, LsmStorageOptions::default()).unwrap();

    let value = "1".repeat(1024); // 1KB

    // approximately 6MB
    for i in 0..6000 {
        storage
            .put(format!("{i}").as_bytes(), value.as_bytes())
            .unwrap();
    }

    std::thread::sleep(Duration::from_millis(500));

    assert!(!storage.inner.state.read().l0_sstables.is_empty());
}

#[test]
fn test_task3_sst_filter() {
    let dir = tempdir().unwrap();
    let storage =
        Arc::new(LsmStorageInner::open(&dir, LsmStorageOptions::default()).unwrap());

    for i in 1..=10000 {
        if i % 1000 == 0 {
            sync(&storage);
        }
        storage
            .put(format!("{:05}", i).as_bytes(), b"2333333")
            .unwrap();
    }

    let iter = storage.scan(Bound::Unbounded, Bound::Unbounded).unwrap();
    assert!(
        iter.num_active_iterators() >= 10,
        "did you implement num_active_iterators? current active iterators = {}",
        iter.num_active_iterators()
    );
    let max_num = iter.num_active_iterators();
    let iter = storage
        .scan(
            Bound::Excluded(format!("{:05}", 10000).as_bytes()),
            Bound::Unbounded,
        )
        .unwrap();
    assert!(iter.num_active_iterators() < max_num);
    let min_num = iter.num_active_iterators();
    let iter = storage
        .scan(
            Bound::Unbounded,
            Bound::Excluded(format!("{:05}", 1).as_bytes()),
        )
        .unwrap();
    assert_eq!(iter.num_active_iterators(), min_num);
    let iter = storage
        .scan(
            Bound::Unbounded,
            Bound::Included(format!("{:05}", 0).as_bytes()),
        )
        .unwrap();
    assert_eq!(iter.num_active_iterators(), min_num);
    let iter = storage
        .scan(
            Bound::Included(format!("{:05}", 10001).as_bytes()),
            Bound::Unbounded,
        )
        .unwrap();
    assert_eq!(iter.num_active_iterators(), min_num);
    let iter = storage
        .scan(
            Bound::Included(format!("{:05}", 5000).as_bytes()),
            Bound::Excluded(format!("{:05}", 6000).as_bytes()),
        )
        .unwrap();
    assert!(min_num <= iter.num_active_iterators() && iter.num_active_iterators() < max_num);
}

}
