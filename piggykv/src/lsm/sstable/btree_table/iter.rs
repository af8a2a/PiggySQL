use bytes::Bytes;
use std::collections::btree_map::Range;
use std::collections::Bound;

use crate::lsm::iterator::{Iter, Seek, SeekIter};
use crate::lsm::mem_table::KeyValue;
use crate::KernelResult;

use super::BTreeTable;

pub(crate) struct BTreeTableIter<'a> {
    inner: Option<Range<'a, Bytes, KeyValue>>,
    table: &'a BTreeTable,
}

impl<'a> BTreeTableIter<'a> {
    pub(crate) fn new(table: &'a BTreeTable) -> BTreeTableIter<'a> {
        let mut iter = BTreeTableIter { inner: None, table };
        iter._seek(Seek::First);
        iter
    }

    fn _seek(&mut self, seek: Seek) {
        self.inner = match seek {
            Seek::First => Some(
                self.table
                    .inner
                    .range::<Bytes, (Bound<Bytes>, Bound<Bytes>)>((
                        Bound::Unbounded,
                        Bound::Unbounded,
                    )),
            ),
            Seek::Last => None,
            Seek::Backward(key) => Some(
                self.table
                    .inner
                    .range::<Bytes, (Bound<Bytes>, Bound<Bytes>)>((
                        Bound::Included(Bytes::copy_from_slice(key)),
                        Bound::Unbounded,
                    )),
            ),
        };
    }
}

impl<'a> Iter<'a> for BTreeTableIter<'a> {
    type Item = KeyValue;

    fn try_next(&mut self) -> KernelResult<Option<Self::Item>> {
        Ok(self
            .inner
            .as_mut()
            .and_then(|iter| iter.next())
            .map(item_clone))
    }

    fn is_valid(&self) -> bool {
        true
    }
}

impl<'a> SeekIter<'a> for BTreeTableIter<'a> {
    fn seek(&mut self, seek: Seek<'_>) -> KernelResult<()> {
        self._seek(seek);

        Ok(())
    }
}

fn item_clone((_, value): (&Bytes, &KeyValue)) -> KeyValue {
    value.clone()
}
