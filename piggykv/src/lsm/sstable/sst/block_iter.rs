
use bytes::Bytes;

use crate::{lsm::iterator::{Iter, Seek, SeekIter}, KernelResult};

use super::block::{Block, BlockItem, Entry};

/// Block迭代器
///
/// Tips: offset偏移会额外向上偏移一位以使用0作为迭代的下界判断是否向前溢出了
pub(crate) struct BlockIter<'a, T> {
    block: &'a Block<T>,
    entry_len: usize,

    offset: usize,
    buf_shared_key: &'a [u8],
}

impl<'a, T> BlockIter<'a, T>
where
    T: BlockItem,
{
    pub(crate) fn new(block: &'a Block<T>) -> BlockIter<'a, T> {
        let buf_shared_key = block.shared_key_prefix(0, block.restart_shared_len(0));

        BlockIter {
            block,
            entry_len: block.entry_len(),
            offset: 0,
            buf_shared_key,
        }
    }

    fn item(&self) -> (Bytes, T) {
        let offset = self.offset - 1;
        let Entry { key, item, .. } = self.block.get_entry(offset);
        let item_key = if offset % self.block.restart_interval() != 0 {
            Bytes::from([self.buf_shared_key, &key[..]].concat())
        } else {
            key.clone()
        };

        (item_key, item.clone())
    }

    fn offset_move(&mut self, offset: usize, is_seek: bool) -> Option<(Bytes, T)> {
        let block = self.block;
        let restart_interval = block.restart_interval();

        let old_offset = self.offset;
        self.offset = offset;

        (offset > 0 && offset < self.entry_len + 1)
            .then(|| {
                let real_offset = offset - 1;
                if old_offset - 1 / restart_interval != real_offset / restart_interval {
                    self.buf_shared_key =
                        block.shared_key_prefix(real_offset, block.restart_shared_len(real_offset));
                }
                (!is_seek).then(|| self.item())
            })
            .flatten()
    }
}

// impl<'a, V> ForwardIter<'a> for BlockIter<'a, V>
// where
//     V: Sync + Send + BlockItem,
// {
//     fn try_prev(&mut self) -> KernelResult<Option<Self::Item>> {
//         Ok((self.is_valid() || self.offset == self.entry_len + 1)
//             .then(|| self.offset_move(self.offset - 1, false))
//             .flatten())
//     }
// }

impl<'a, V> Iter<'a> for BlockIter<'a, V>
where
    V: Sync + Send + BlockItem,
{
    type Item = (Bytes, V);

    fn try_next(&mut self) -> KernelResult<Option<Self::Item>> {
        Ok((self.is_valid() || self.offset == 0)
            .then(|| self.offset_move(self.offset + 1, false))
            .flatten())
    }

    fn is_valid(&self) -> bool {
        self.offset > 0 && self.offset <= self.entry_len
    }
}

impl<'a, V> SeekIter<'a> for BlockIter<'a, V>
where
    V: Sync + Send + BlockItem,
{
    fn seek(&mut self, seek: Seek<'_>) -> KernelResult<()> {
        match seek {
            Seek::First => Some(0),
            Seek::Last => Some(self.entry_len + 1),
            Seek::Backward(key) => match self.block.binary_search(key) {
                Ok(index) => Some(index),
                Err(index) => (index < self.entry_len).then_some(index),
            },
        }
        .and_then(|index| self.offset_move(index, true));

        Ok(())
    }
}