pub(crate) mod iter;

use bytes::Bytes;
use std::collections::BTreeMap;

use crate::{lsm::{iterator::SeekIter, mem_table::KeyValue}, KernelResult};

use self::iter::BTreeTableIter;

use super::Table;

pub(crate) struct BTreeTable {
    level: usize,
    gen: i64,
    len: usize,
    inner: BTreeMap<Bytes, KeyValue>,
}

impl BTreeTable {
    pub(crate) fn new(level: usize, gen: i64, data: Vec<KeyValue>) -> Self {
        let len = data.len();
        let inner = BTreeMap::from_iter(
            data.into_iter()
                .map(|(key, value)| (key.clone(), (key, value))),
        );

        BTreeTable {
            level,
            gen,
            len,
            inner,
        }
    }
}

impl Table for BTreeTable {
    fn query(&self, key: &[u8]) -> KernelResult<Option<KeyValue>> {
        Ok(self.inner.get(key).cloned())
    }

    fn len(&self) -> usize {
        self.len
    }

    fn size_of_disk(&self) -> u64 {
        0
    }

    fn gen(&self) -> i64 {
        self.gen
    }

    fn level(&self) -> usize {
        self.level
    }

    #[allow(clippy::todo)]
    fn iter<'a>(
        &'a self,
    ) -> KernelResult<Box<dyn SeekIter<'a, Item = KeyValue> + 'a + Send + Sync>>
    {
        Ok(Box::new(BTreeTableIter::new(self)))
    }
}
