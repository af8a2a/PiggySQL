use crate::lsm::compactor::CompactTask;
use crate::lsm::iterator::merging_iter::MergingIter;
use crate::lsm::iterator::{Iter, Seek, SeekIter};
use crate::lsm::mem_table::{KeyValue, MemTable};
use crate::lsm::query_and_compaction;
use crate::lsm::storage::{ Sequence, StoreInner};
use crate::lsm::version::iter::VersionIter;
use crate::lsm::version::Version;
use crate::KernelResult;
use crate::KernelError;
use bytes::Bytes;
use core::slice::SlicePattern;
use itertools::Itertools;
use std::collections::btree_map::Range;
use std::collections::{BTreeMap, Bound};
use std::ptr::NonNull;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use tokio::sync::mpsc::error::TrySendError;
use tokio::sync::mpsc::Sender;

use super::PiggyKV;

unsafe impl Send for BufPtr {}
unsafe impl Sync for BufPtr {}

struct BufPtr(NonNull<Vec<KeyValue>>);

pub enum CheckType {
    Optimistic,
}

pub struct Transaction {
    store_inner: Arc<StoreInner>,
    compactor_tx: Sender<CompactTask>,

    version: Arc<Version>,
    seq_id: i64,
    check_type: CheckType,

    write_buf: Option<BTreeMap<Bytes, Option<Bytes>>>,
}

impl Transaction {
    pub(crate) async fn new(storage: &PiggyKV, check_type: CheckType) -> Self {
        let _ = storage.mem_table().tx_count.fetch_add(1, Ordering::Release);

        Transaction {
            store_inner: Arc::clone(&storage.inner),
            version: storage.current_version().await,
            compactor_tx: storage.compactor_tx.clone(),

            seq_id: Sequence::create(),
            write_buf: None,
            check_type,
        }
    }

    fn write_buf_or_init(&mut self) -> &mut BTreeMap<Bytes, Option<Bytes>> {
        self.write_buf.get_or_insert_with(BTreeMap::new)
    }

    /// 通过Key获取对应的Value
    ///
    /// 此处不需要等待压缩，因为在Transaction存活时不会触发Compaction
    #[inline]
    pub fn get(&self, key: &[u8]) -> KernelResult<Option<Bytes>> {
        if let Some(value) = self.write_buf.as_ref().and_then(|buf| buf.get(key)) {
            return Ok(value.clone());
        }

        if let Some((_, value)) = self.mem_table().find_with_sequence_id(key, self.seq_id) {
            return Ok(value);
        }

        if let Some((_, value)) = query_and_compaction(key, &self.version, &self.compactor_tx)? {
            return Ok(value);
        }

        Ok(None)
    }

    #[inline]
    pub fn set(&mut self, key: Bytes, value: Bytes) {
        let _ignore = self.write_buf_or_init().insert(key, Some(value));
    }

    #[inline]
    pub fn remove(&mut self, key: &[u8]) -> KernelResult<()> {
        let _ = self.get(key)?.ok_or(KernelError::KeyNotFound)?;
        let bytes = Bytes::copy_from_slice(key);
        let _ignore = self.write_buf_or_init().insert(bytes, None);

        Ok(())
    }

    #[inline]
    pub async fn commit(mut self) -> KernelResult<()> {
        if let Some(buf) = self.write_buf.take() {
            let batch_data = buf.into_iter().collect_vec();

            match self.check_type {
                CheckType::Optimistic => {
                    if self
                        .mem_table()
                        .check_key_conflict(&batch_data, self.seq_id)
                    {
                        return Err(KernelError::RepeatedWrite);
                    }
                }
            }

            let is_exceeds = self
                .store_inner
                .mem_table
                .insert_batch_data(batch_data, Sequence::create())?;

            if is_exceeds {
                if let Err(TrySendError::Closed(_)) =
                    self.compactor_tx.try_send(CompactTask::Flush(None))
                {
                    return Err(KernelError::ChannelClose);
                }
            }
        }

        Ok(())
    }

    fn mem_table(&self) -> &MemTable {
        &self.store_inner.mem_table
    }

    #[inline]
    pub fn disk_iter(&self) -> KernelResult<VersionIter> {
        VersionIter::new(&self.version)
    }

    #[inline]
    pub fn iter<'a>(
        &'a self,
        min: Bound<&[u8]>,
        max: Bound<&[u8]>,
    ) -> KernelResult<TransactionIter> {
        let mut vec_iter: Vec<Box<dyn Iter<'a, Item = KeyValue> + 'a + Send + Sync>> =
            Vec::with_capacity(3);

        if let Some(write_buf) = &self.write_buf {
            let range = write_buf.range::<Bytes, (Bound<&Bytes>, Bound<&Bytes>)>((
                min.map(Bytes::copy_from_slice).as_ref(),
                max.map(Bytes::copy_from_slice).as_ref(),
            ));

            vec_iter.push(Box::new(InnerIter { iter: range }));
        }

        let mem_buf = self.mem_table().range_scan(min, max, Some(self.seq_id));
        let mem_buf_ptr = BufPtr(Box::leak(Box::new(mem_buf)).into());

        vec_iter.push(Box::new(unsafe {
            BufIter {
                inner: mem_buf_ptr.0.as_ref(),
                pos: 0,
            }
        }));
        let mut ver_iter = VersionIter::new(&self.version)?;

        match &min {
            Bound::Included(key) | Bound::Excluded(key) => {
                ver_iter.seek(Seek::Backward(key.as_slice()))?;
            }
            Bound::Unbounded => (),
        };
        vec_iter.push(Box::new(ver_iter));

        Ok(TransactionIter {
            inner: MergingIter::new(vec_iter)?,
            max: max.map(Bytes::copy_from_slice),
            mem_buf_ptr,
            is_overed: false,
            min: min.map(Bytes::copy_from_slice),
            is_inited: false,
        })
    }
}

impl Drop for Transaction {
    #[inline]
    fn drop(&mut self) {
        let _ = self.mem_table().tx_count.fetch_sub(1, Ordering::Release);
    }
}

unsafe impl Sync for TransactionIter<'_> {}

unsafe impl Send for TransactionIter<'_> {}

pub struct TransactionIter<'a> {
    inner: MergingIter<'a>,
    mem_buf_ptr: BufPtr,

    min: Bound<Bytes>,
    max: Bound<Bytes>,
    is_overed: bool,
    is_inited: bool,
}

impl<'a> Iter<'a> for TransactionIter<'a> {
    type Item = KeyValue;

    #[inline]
    fn try_next(&mut self) -> KernelResult<Option<Self::Item>> {
        if self.is_overed {
            return Ok(None);
        }
        let mut item = self.inner.try_next()?;

        if !self.is_inited {
            loop {
                if match &self.min {
                    Bound::Included(key) => {
                        !matches!(item.as_ref().map(|data| data.0 < key), Some(true))
                    }
                    Bound::Excluded(key) => {
                        !matches!(item.as_ref().map(|data| data.0 <= key), Some(true))
                    }
                    Bound::Unbounded => true,
                } {
                    break;
                }
                item = self.inner.try_next()?;
            }
            self.is_inited = true
        }
        let option = match &self.max {
            Bound::Included(key) => item.and_then(|data| (data.0 <= key).then_some(data)),
            Bound::Excluded(key) => item.and_then(|data| (data.0 < key).then_some(data)),
            Bound::Unbounded => item,
        };
        self.is_overed = option.is_none();

        Ok(option)
    }

    #[inline]
    fn is_valid(&self) -> bool {
        self.inner.is_valid()
    }
}

impl<'a> Iterator for TransactionIter<'a>{
    type Item = KeyValue;

    fn next(&mut self) -> Option<Self::Item> {
        if self.is_valid(){
            self.try_next().unwrap()
        }else{
            None
        }
    }
}
impl Drop for TransactionIter<'_> {
    #[inline]
    fn drop(&mut self) {
        unsafe { drop(Box::from_raw(self.mem_buf_ptr.0.as_ptr())) }
    }
}

struct BufIter<'a> {
    inner: &'a Vec<KeyValue>,
    pos: usize,
}

struct InnerIter<'a> {
    iter: Range<'a, Bytes, Option<Bytes>>,
}

impl<'a> Iter<'a> for BufIter<'a> {
    type Item = KeyValue;

    fn try_next(&mut self) -> KernelResult<Option<Self::Item>> {
        Ok(self.is_valid().then(|| {
            let item = self.inner[self.pos].clone();
            self.pos += 1;
            item
        }))
    }

    fn is_valid(&self) -> bool {
        self.pos < self.inner.len()
    }
}

impl<'a> Iter<'a> for InnerIter<'a> {
    type Item = KeyValue;

    fn try_next(&mut self) -> KernelResult<Option<Self::Item>> {
        Ok(self
            .iter
            .next()
            .map(|(key, value)| (key.clone(), value.clone())))
    }

    fn is_valid(&self) -> bool {
        true
    }
}
