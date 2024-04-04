use crate::lsm::iterator::{Iter, Seek, SeekIter};
use crate::lsm::mem_table::KeyValue;
use crate::KernelResult;
use bytes::Bytes;
use std::cmp::Ordering;
use std::collections::BTreeMap;

/// 用于取值以及对应的Iter下标
/// 通过序号进行同值优先获取
#[derive(Eq, PartialEq, Debug)]
struct IterKey {
    num: usize,
    key: Bytes,
}

impl PartialOrd<Self> for IterKey {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for IterKey {
    fn cmp(&self, other: &Self) -> Ordering {
        self.key
            .cmp(&other.key)
            .then_with(|| self.num.cmp(&other.num))
    }
}

struct InnerIter {
    map_buf: BTreeMap<IterKey, Option<Bytes>>,
    pre_key: Option<Bytes>,
}

pub(crate) struct MergingIter<'a> {
    vec_iter: Vec<Box<dyn Iter<'a, Item = KeyValue> + 'a + Send + Sync>>,
    inner: InnerIter,
}

pub(crate) struct SeekMergingIter<'a> {
    vec_iter: Vec<Box<dyn SeekIter<'a, Item = KeyValue> + 'a + Send + Sync>>,
    inner: InnerIter,
}

impl<'a> MergingIter<'a> {
    #[allow(clippy::mutable_key_type)]
    pub(crate) fn new(
        mut vec_iter: Vec<Box<dyn Iter<'a, Item = KeyValue> + 'a + Send + Sync>>,
    ) -> KernelResult<Self> {
        let mut map_buf = BTreeMap::new();

        for (num, iter) in vec_iter.iter_mut().enumerate() {
            if let Some(item) = iter.try_next()? {
                InnerIter::buf_map_insert(&mut map_buf, num, item);
            }
        }

        Ok(MergingIter {
            vec_iter,
            inner: InnerIter {
                map_buf,
                pre_key: None,
            },
        })
    }
}

impl<'a> SeekMergingIter<'a> {
    #[allow(clippy::mutable_key_type)]
    pub(crate) fn new(
        mut vec_iter: Vec<Box<dyn SeekIter<'a, Item = KeyValue> + 'a + Send + Sync>>,
    ) -> KernelResult<Self> {
        let mut map_buf = BTreeMap::new();

        for (num, iter) in vec_iter.iter_mut().enumerate() {
            if let Some(item) = iter.try_next()? {
                InnerIter::buf_map_insert(&mut map_buf, num, item);
            }
        }

        Ok(SeekMergingIter {
            vec_iter,
            inner: InnerIter {
                map_buf,
                pre_key: None,
            },
        })
    }
}

macro_rules! is_valid {
    ($vec_iter:expr) => {
        $vec_iter
            .iter()
            .map(|iter| iter.is_valid())
            .all(|is_valid| is_valid)
    };
}

impl<'a> Iter<'a> for MergingIter<'a> {
    type Item = KeyValue;

    fn try_next(&mut self) -> KernelResult<Option<Self::Item>> {
        self.inner.try_next_1(&mut self.vec_iter)
    }

    fn is_valid(&self) -> bool {
        is_valid!(&self.vec_iter)
    }
}

impl<'a> Iter<'a> for SeekMergingIter<'a> {
    type Item = KeyValue;

    fn try_next(&mut self) -> KernelResult<Option<Self::Item>> {
        self.inner.try_next_2(&mut self.vec_iter)
    }

    fn is_valid(&self) -> bool {
        is_valid!(&self.vec_iter)
    }
}

macro_rules! impl_try_next {
    ($func:ident, $vec_iter:ty) => {
        impl InnerIter {
            fn $func(&mut self, vec_iter: &mut [$vec_iter]) -> KernelResult<Option<KeyValue>> {
                while let Some((IterKey { num, key }, value)) = self.map_buf.pop_first() {
                    if let Some(item) = vec_iter[num].try_next()? {
                        Self::buf_map_insert(&mut self.map_buf, num, item);
                    }

                    // 跳过重复元素
                    if let Some(pre_key) = &self.pre_key {
                        if pre_key == &key {
                            continue;
                        }
                    }
                    self.pre_key = Some(key.clone());

                    return Ok(Some((key, value)));
                }

                Ok(None)
            }
        }
    };
}

impl_try_next!(
    try_next_1,
    Box<dyn Iter<'_, Item = KeyValue> + '_ + Send + Sync>
);
impl_try_next!(
    try_next_2,
    Box<dyn SeekIter<'_, Item = KeyValue> + '_ + Send + Sync>
);

impl InnerIter {
    #[allow(clippy::mutable_key_type)]
    fn buf_map_insert(
        seek_map: &mut BTreeMap<IterKey, Option<Bytes>>,
        num: usize,
        (key, value): KeyValue,
    ) {
        let _ = seek_map.insert(IterKey { num, key }, value);
    }
}

impl<'a> SeekIter<'a> for SeekMergingIter<'a> {
    #[allow(clippy::mutable_key_type)]
    fn seek(&mut self, seek: Seek<'_>) -> KernelResult<()> {
        if let Seek::Last = seek {
            self.inner.map_buf.clear();
        } else {
            let mut seek_map = BTreeMap::new();

            for (num, iter) in self.vec_iter.iter_mut().enumerate() {
                iter.seek(seek)?;

                if let Some(item) = iter.try_next()? {
                    InnerIter::buf_map_insert(&mut seek_map, num, item);
                }
            }

            self.inner.map_buf = seek_map;
        }

        Ok(())
    }
}