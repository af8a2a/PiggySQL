use std::{
    cmp::Ordering,
    io::Cursor,
    mem,
    ops::Bound,
    sync::{atomic::AtomicUsize, Arc},
};

use bytes::Bytes;
use itertools::Itertools;
use parking_lot::Mutex;
use skiplist::{skipmap, SkipMap};
use std::sync::atomic::Ordering::Acquire;

use crate::{io::IoWriter, lsm::storage::Gen, KernelResult};

use super::{
    iterator::{Iter, Seek, SeekIter},
    sstable::sst::block::{Entry, Value},
    storage::{Config, Sequence},
    trigger::{Trigger, TriggerFactory},
    wal::{LogLoader, LogWriter, DEFAULT_WAL_PATH},
};

pub(crate) type MemMap = SkipMap<InternalKey, Option<Bytes>>;

pub(crate) type KeyValue = (Bytes, Option<Bytes>);
const SEQ_MAX: i64 = i64::MAX;

pub(crate) fn key_value_bytes_len(key_value: &KeyValue) -> usize {
    key_value.0.len() + key_value.1.as_ref().map(Bytes::len).unwrap_or(0)
}

#[derive(PartialEq, Eq, Debug, Clone)]
pub(crate) struct InternalKey {
    key: Bytes,
    seq_id: i64,
}

impl PartialOrd<Self> for InternalKey {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for InternalKey {
    fn cmp(&self, other: &Self) -> Ordering {
        self.key
            .cmp(&other.key)
            .then_with(|| self.seq_id.cmp(&other.seq_id))
    }
}

impl InternalKey {
    pub(crate) fn new(key: Bytes) -> Self {
        InternalKey {
            key,
            seq_id: Sequence::create(),
        }
    }

    pub(crate) fn new_with_seq(key: Bytes, seq_id: i64) -> Self {
        InternalKey { key, seq_id }
    }

    pub(crate) fn get_key(&self) -> &Bytes {
        &self.key
    }
}

pub(crate) struct MemMapIter<'a> {
    mem_map:&'a MemMap,

    prev_item: Option<(Bytes, Option<Bytes>)>,
    iter: Option<skipmap::Iter<'a, InternalKey, Option<Bytes>>>,
}

impl<'a> MemMapIter<'a> {
    #[allow(dead_code)]
    pub(crate) fn new(mem_map: &'a MemMap) -> Self {
        Self {
            mem_map,
            prev_item: None,
            iter: Some(mem_map.iter()),
        }
    }
}

impl<'a> Iter<'a> for MemMapIter<'a> {
    type Item = KeyValue;

    fn try_next(&mut self) -> KernelResult<Option<Self::Item>> {
        if let Some(iter) = &mut self.iter {
            for (InternalKey { key, .. }, value) in iter.by_ref() {
                if let Some(prev_item) = &self.prev_item {
                    if key != &prev_item.0 {
                        return Ok(mem::replace(
                            &mut self.prev_item,
                            Some((key.clone(), value.clone())),
                        ));
                    }
                }
                self.prev_item = Some((key.clone(), value.clone()));
            }

            return Ok(self.prev_item.take());
        }

        Ok(None)
    }

    fn is_valid(&self) -> bool {
        true
    }
}

impl<'a> SeekIter<'a> for MemMapIter<'a> {
    fn seek(&mut self, seek: Seek<'_>) -> KernelResult<()> {
        self.prev_item = None;

        if let Seek::Last = seek {
            self.iter = None;
        } else {
            self.iter = match seek {
                Seek::First => Some(self.mem_map.iter()),
                Seek::Last => None,
                Seek::Backward(seek_key) => Some(self.mem_map.range(
                    Bound::Included(&InternalKey::new_with_seq(
                        Bytes::copy_from_slice(seek_key),
                        0,
                    )),
                    Bound::Unbounded,
                )),
            };
        }

        Ok(())
    }
}

pub(crate) struct MemTable {
    inner: Mutex<TableInner>,
    pub(crate) tx_count: AtomicUsize,
}

pub(crate) struct TableInner {
    pub(crate) _mem: MemMap,
    pub(crate) _immut: Option<Arc<MemMap>>,
    /// WAL载入器
    ///
    /// 用于异常停机时MemTable的恢复
    /// 同时当Level 0的SSTable异常时，可以尝试恢复
    log_loader: LogLoader,
    log_writer: (LogWriter<Box<dyn IoWriter>>, i64),
    trigger: Box<dyn Trigger + Send>,
}

macro_rules! check_count {
    ($count:ident) => {
        if 0 != $count.load(Acquire) {
            std::hint::spin_loop();
            continue;
        }
    };
}

macro_rules! range_iter {
    ($map:expr, $min_key:expr, $max_key:expr, $option_seq:expr) => {
        $map.range($min_key.as_ref(), $max_key.as_ref())
            .rev()
            .filter(|(InternalKey { seq_id, .. }, _)| {
                $option_seq.map_or(true, |current_seq| &current_seq >= seq_id)
            })
            .map(|(internal_key, value)| (&internal_key.key, value))
    };
}

impl MemTable {
    pub(crate) fn new(config: &Config) -> KernelResult<Self> {
        let mut log_records = Vec::new();
        let (log_loader, log_gen) = LogLoader::reload(
            config.path(),
            (DEFAULT_WAL_PATH, None),
            config.wal_io_type,
            &mut log_records,
            |bytes, records| {
                for (_, Entry { key, item, .. }) in
                    Entry::<Value>::batch_decode(&mut Cursor::new(mem::take(bytes)))?
                {
                    records.push((InternalKey::new_with_seq(key, 0), item.bytes));
                }

                Ok(())
            },
        )?;
        let log_writer = (log_loader.writer(log_gen)?, log_gen);
        // Q: 为什么INIT_SEQ作为Seq id?
        // A: 因为此处是当存在有停机异常时使用wal恢复数据,此处也不存在有Version(VersionStatus的初始化在此代码之后)
        // 因此不会影响Version的读取顺序
        let mem_map = MemMap::from_iter(log_records);
        let (trigger_type, threshold) = config.minor_trigger_with_threshold;

        Ok(MemTable {
            inner: Mutex::new(TableInner {
                _mem: mem_map,
                _immut: None,
                log_loader,
                log_writer,
                trigger: TriggerFactory::create(trigger_type, threshold),
            }),
            tx_count: AtomicUsize::new(0),
        })
    }

    pub(crate) fn check_key_conflict(&self, kvs: &[KeyValue], seq_id: i64) -> bool {
        let inner = self.inner.lock();

        for (key, _) in kvs {
            let internal_key = InternalKey::new_with_seq(key.clone(), seq_id);

            if let Some(true) = inner
                ._mem
                .lower_bound(Bound::Excluded(&internal_key))
                .map(|(lower_key, _)| lower_key.key == key)
            {
                return true;
            }
        }

        false
    }

    /// 插入并判断是否溢出
    ///
    /// 插入时不会去除重复键值，而是进行追加
    pub(crate) fn insert_data(&self, data: KeyValue) -> KernelResult<bool> {
        let mut inner = self.inner.lock();

        let _ = inner
            .log_writer
            .0
            .add_record(&data_to_bytes(data.clone())?)?;

        inner.trigger.item_process(&data);
        let (key, value) = data;
        let _ = inner._mem.insert(InternalKey::new(key), value);

        Ok(inner.trigger.is_exceeded())
    }

    /// Tips: 当数据在插入mem_table中停机，则不会存入日志中
    pub(crate) fn insert_batch_data(
        &self,
        vec_data: Vec<KeyValue>,
        seq_id: i64,
    ) -> KernelResult<bool> {
        let mut inner = self.inner.lock();

        let mut buf = Vec::new();
        for item in vec_data {
            let (key, value) = item.clone();
            inner.trigger.item_process(&item);

            let _ = inner
                ._mem
                .insert(InternalKey::new_with_seq(key, seq_id), value);
            buf.append(&mut data_to_bytes(item)?);
        }
        let _ = inner.log_writer.0.add_record(&buf)?;

        Ok(inner.trigger.is_exceeded())
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.inner.lock()._mem.is_empty()
    }

    pub(crate) fn len(&self) -> usize {
        self.inner.lock()._mem.len()
    }

    pub(crate) fn log_loader_clone(&self) -> LogLoader {
        self.inner.lock().log_loader.clone()
    }

    /// MemTable将数据弹出并转移到immut table中  (弹出数据为转移至immut table中数据的迭代器)
    pub(crate) fn swap(&self) -> KernelResult<Option<(i64, Vec<KeyValue>)>> {
        let count = &self.tx_count;

        loop {
            check_count!(count);

            let mut inner = self.inner.lock();
            // 二重检测防止lock时(前)突然出现事务
            // 当lock后，即使出现事务，会因为lock已被Compactor获取而无法读写，
            // 因此不会对读写进行干扰
            // 并且事务即使在lock后出现，所持有的seq为该压缩之前，
            // 也不会丢失该seq的_mem，因为转移到了_immut，可以从_immut得到对应seq的数据
            check_count!(count);

            return if !inner._mem.is_empty() {
                inner.trigger.reset();

                let mut vec_data = inner
                    ._mem
                    .iter()
                    .map(|(k, v)| (k.key.clone(), v.clone()))
                    // rev以使用最后(最新)的key
                    .rev()
                    .unique_by(|(k, _)| k.clone())
                    .collect_vec();

                vec_data.reverse();

                inner._immut = Some(Arc::new(mem::replace(&mut inner._mem, SkipMap::new())));

                let new_gen = Gen::create();
                let new_writer = (inner.log_loader.writer(new_gen)?, new_gen);
                let (mut old_writer, old_gen) = mem::replace(&mut inner.log_writer, new_writer);
                old_writer.flush()?;

                Ok(Some((old_gen, vec_data)))
            } else {
                Ok(None)
            };
        }
    }

    pub(crate) fn find(&self, key: &[u8]) -> Option<KeyValue> {
        // 填充SEQ_MAX使其变为最高位以尽可能获取最新数据
        let internal_key = InternalKey::new_with_seq(Bytes::copy_from_slice(key), SEQ_MAX);
        let inner = self.inner.lock();

        Self::find_(&internal_key, &inner._mem).or_else(|| {
            inner
                ._immut
                .as_ref()
                .and_then(|mem_map| Self::find_(&internal_key, mem_map))
        })
    }

    /// 查询时附带seq_id进行历史数据查询
    pub(crate) fn find_with_sequence_id(&self, key: &[u8], seq_id: i64) -> Option<KeyValue> {
        let internal_key = InternalKey::new_with_seq(Bytes::copy_from_slice(key), seq_id);
        let inner = self.inner.lock();

        if let Some(key_value) = MemTable::find_(&internal_key, &inner._mem) {
            Some(key_value)
        } else if let Some(mem_map) = &inner._immut {
            MemTable::find_(&internal_key, mem_map)
        } else {
            None
        }
    }

    fn find_(internal_key: &InternalKey, mem_map: &MemMap) -> Option<KeyValue> {
        mem_map
            .upper_bound(Bound::Included(internal_key))
            .and_then(|(upper_key, value)| {
                let key = internal_key.get_key();
                (key == &upper_key.key).then(|| (key.clone(), value.clone()))
            })
    }

    fn _range_scan(
        inner: &TableInner,
        min: Bound<&[u8]>,
        max: Bound<&[u8]>,
        option_seq: Option<i64>,
    ) -> Vec<KeyValue> {
        fn to_internal_key(
            bound: &Bound<&[u8]>,
            included: i64,
            excluded: i64,
        ) -> Bound<InternalKey> {
            bound.map(|key| {
                InternalKey::new_with_seq(
                    Bytes::copy_from_slice(key),
                    if let Bound::Included(_) = &bound {
                        included
                    } else {
                        excluded
                    },
                )
            })
        }
        let inner = unsafe {
            // Tips: make sure the `mem_iter` and `immut_mem_iter` destruct in this method
            mem::transmute::<&TableInner, &'static TableInner>(inner)
        };

        let min_key = to_internal_key(&min, i64::MIN, i64::MAX);
        let max_key = to_internal_key(&max, i64::MAX, i64::MIN);

        let mut merged = Vec::new();
        let fn_push =
            |results: &mut Vec<KeyValue>, item: &mut Option<(&Bytes, &Option<Bytes>)>, new_item| {
                if let Some((internal_key, value)) = mem::replace(item, new_item) {
                    Self::duplicates_push(results, internal_key, value);
                }
            };
        let mut mem_iter = range_iter!(inner._mem, min_key, max_key, option_seq);

        if let Some(immut) = &inner._immut {
            let mut immut_mem_iter = range_iter!(immut, min_key, max_key, option_seq);
            let (mut mem_current, mut immut_mem_current) = (mem_iter.next(), immut_mem_iter.next());

            while mem_current.is_some() && immut_mem_current.is_some() {
                match mem_current
                    .as_ref()
                    .unwrap()
                    .0
                    .cmp(immut_mem_current.as_ref().unwrap().0)
                {
                    Ordering::Greater => fn_push(&mut merged, &mut mem_current, mem_iter.next()),
                    Ordering::Less => {
                        fn_push(&mut merged, &mut immut_mem_current, immut_mem_iter.next())
                    }
                    Ordering::Equal => {
                        fn_push(&mut merged, &mut mem_current, mem_iter.next());

                        immut_mem_current = immut_mem_iter.next();
                    }
                }
            }

            if let Some((internal_key, value)) = mem_current {
                Self::duplicates_push(&mut merged, internal_key, value);
            }
            if let Some((internal_key, value)) = immut_mem_current {
                Self::duplicates_push(&mut merged, internal_key, value);
            }
            // one of the two is empty
            mem_iter
                .chain(immut_mem_iter)
                .for_each(|(internal_key, value)| {
                    Self::duplicates_push(&mut merged, internal_key, value)
                });
        } else {
            mem_iter.for_each(|(internal_key, value)| {
                Self::duplicates_push(&mut merged, internal_key, value)
            });
        }

        merged.reverse();
        assert!(merged.is_sorted_by_key(|(k, _)| k));
        assert!(merged.iter().all_unique());
        merged
    }

    fn duplicates_push(results: &mut Vec<KeyValue>, key: &Bytes, value: &Option<Bytes>) {
        if !matches!(
            results.last().map(|(last_key, _)| last_key == key),
            Some(true)
        ) {
            results.push((key.clone(), value.clone()))
        }
    }

    pub(crate) fn range_scan(
        &self,
        min: Bound<&[u8]>,
        max: Bound<&[u8]>,
        option_seq: Option<i64>,
    ) -> Vec<KeyValue> {
        let inner = self.inner.lock();

        Self::_range_scan(&inner, min, max, option_seq)
    }
}

pub(crate) fn data_to_bytes(data: KeyValue) -> KernelResult<Vec<u8>> {
    let (key, value) = data.clone();
    let mut bytes = Vec::new();

    Entry::new(0, key.len(), key, Value::from(value)).encode(&mut bytes)?;
    Ok(bytes)
}
