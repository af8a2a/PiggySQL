use std::ops::Bound;

use crate::errors::Result;
use bytes::Bytes;
use tracing::debug;

use super::iterators::concat_iterator::SstConcatIterator;
use super::iterators::merge_iterator::MergeIterator;
use super::iterators::two_merge_iterator::TwoMergeIterator;
use super::iterators::StorageIterator;
use super::mem_table::MemTableIterator;
use super::table::SsTableIterator;

/// Represents the internal type for an LSM iterator. This type will be changed across the tutorial for multiple times.
type LsmIteratorInner = TwoMergeIterator<
    TwoMergeIterator<MergeIterator<MemTableIterator>, MergeIterator<SsTableIterator>>,
    MergeIterator<SstConcatIterator>,
>;

pub struct LsmIterator {
    inner: LsmIteratorInner,
    end_bound: Bound<Bytes>,
    is_valid: bool,
    read_ts: u64,
    prev_key: Vec<u8>,
}

impl LsmIterator {
    pub(crate) fn new(
        iter: LsmIteratorInner,
        end_bound: Bound<Bytes>,
        read_ts: u64,
    ) -> Result<Self> {
        let mut iter = Self {
            is_valid: iter.is_valid(),
            inner: iter,
            end_bound,
            read_ts,
            prev_key: Vec::new(),
        };
        iter.move_to_key()?;
        Ok(iter)
    }

    fn next_inner(&mut self) -> Result<()> {
        self.inner._next()?;
        if !self.inner.is_valid() {
            self.is_valid = false;
            return Ok(());
        }

        match self.end_bound.as_ref() {
            Bound::Unbounded => {}
            Bound::Included(key) => self.is_valid = self.inner.key().key_ref() <= key.as_ref(),
            Bound::Excluded(key) => self.is_valid = self.inner.key().key_ref() < key.as_ref(),
        }
        Ok(())
    }

    fn move_to_key(&mut self) -> Result<()> {
        loop {
            while self.inner.is_valid() && self.inner.key().key_ref() == self.prev_key {
                self.next_inner()?;
            }
            if !self.inner.is_valid() {
                break;
            }
            self.prev_key.clear();
            self.prev_key.extend(self.inner.key().key_ref());
            while self.inner.is_valid()
                && self.inner.key().key_ref() == self.prev_key
                && self.inner.key().ts() > self.read_ts
            {
                self.next_inner()?;
            }
            if !self.inner.is_valid() {
                break;
            }
            if self.inner.key().key_ref() != self.prev_key {
                continue;
            }

            if !self.inner.value().is_empty() {
                break;
            }
        }
        Ok(())
    }
}

impl StorageIterator for LsmIterator {
    type KeyType<'a> = &'a [u8];

    fn is_valid(&self) -> bool {
        // //todo
        // let mut res = true;
        // if self.inner.is_valid() {
        //     res = match self.end_bound.as_ref() {
        //         Bound::Unbounded => true,
        //         Bound::Included(key) => self.inner.key().raw_ref() <= key.as_ref(),
        //         Bound::Excluded(key) => self.inner.key().raw_ref() < key.as_ref(),
        //     };
        // }
        self.is_valid
    }

    fn key(&self) -> &[u8] {
        self.inner.key().key_ref()
    }

    fn value(&self) -> &[u8] {
        self.inner.value()
    }

    fn _next(&mut self) -> Result<()> {
        self.next_inner()?;
        self.move_to_key()?;
        Ok(())
    }

    fn num_active_iterators(&self) -> usize {
        self.inner.num_active_iterators()
    }
}

/// A wrapper around existing iterator, will prevent users from calling `next` when the iterator is
/// invalid. If an iterator is already invalid, `next` does not do anything. If `next` returns an error,
/// `is_valid` should return false, and `next` should always return an error.
pub struct FusedIterator<I: StorageIterator> {
    iter: I,
    has_errored: bool,
}

impl<I: StorageIterator> FusedIterator<I> {
    pub fn new(iter: I) -> Self {
        Self {
            iter,
            has_errored: false,
        }
    }
}

impl<I: StorageIterator> StorageIterator for FusedIterator<I> {
    type KeyType<'a> = I::KeyType<'a> where Self: 'a;

    fn is_valid(&self) -> bool {
        !self.has_errored && self.iter.is_valid()
    }

    fn key(&self) -> Self::KeyType<'_> {
        if !self.is_valid() {
            panic!("invalid access to the underlying iterator");
        }
        self.iter.key()
    }

    fn value(&self) -> &[u8] {
        if !self.is_valid() {
            panic!("invalid access to the underlying iterator");
        }
        self.iter.value()
    }

    fn _next(&mut self) -> Result<()> {
        // only move when the iterator is valid and not errored
        if self.has_errored {
            panic!("the iterator is tainted");
        }
        if self.iter.is_valid() {
            if let Err(e) = self.iter._next() {
                self.has_errored = true;
                return Err(e);
            }
        }
        Ok(())
    }

    fn num_active_iterators(&self) -> usize {
        self.iter.num_active_iterators()
    }
}