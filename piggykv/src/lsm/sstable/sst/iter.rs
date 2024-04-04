use crate::{error::KernelError, lsm::{iterator::{Iter, Seek, SeekIter}, mem_table::KeyValue, sstable::Table}, KernelResult};

use super::{block::{BlockType, Index,  Value}, block_iter::BlockIter, SSTable};

pub(crate) struct SSTableIter<'a> {
    ss_table: &'a SSTable,
    data_iter: BlockIter<'a, Value>,
    index_iter: BlockIter<'a, Index>,
}

impl<'a> SSTableIter<'a> {
    pub(crate) fn new(ss_table: &'a SSTable) -> KernelResult<SSTableIter<'a>> {
        let mut index_iter = BlockIter::new(ss_table.index_block()?);
        let index = index_iter.try_next()?.ok_or(KernelError::DataEmpty)?.1;
        let data_iter = Self::data_iter_init(ss_table, index)?;

        Ok(Self {
            ss_table,
            data_iter,
            index_iter,
        })
    }

    fn data_iter_init(ss_table: &'a SSTable, index: Index) -> KernelResult<BlockIter<'a, Value>> {

        let block = {
            ss_table
                .cache
                .get_or_insert((ss_table.gen(), Some(index)), |(_, index)| {
                    let index = (*index).ok_or_else(|| KernelError::DataEmpty)?;
                    ss_table.data_block(index)
                })
                .map(|block_type| match block_type {
                    BlockType::Data(data_block) => Some(data_block),
                    _ => None,
                })?
        }
        .ok_or(KernelError::DataEmpty)?;

        Ok(BlockIter::new(block))
    }

    fn data_iter_seek(&mut self, seek: Seek<'_>, index: Index) -> KernelResult<()> {
        self.data_iter = Self::data_iter_init(self.ss_table, index)?;
        self.data_iter.seek(seek)?;

        Ok(())
    }
}

// impl<'a> ForwardIter<'a> for SSTableIter<'a> {
//     fn try_prev(&mut self) -> KernelResult<Option<Self::Item>> {
//         match self.data_iter.try_prev()? {
//             None => {
//                 if let Some((_, index)) = self.index_iter.try_prev()? {
//                     self.data_iter_seek(Seek::Last, index)?;

//                     Ok(self
//                         .data_iter
//                         .try_prev()?
//                         .map(|(key, value)| (key, value.bytes)))
//                 } else {
//                     Ok(None)
//                 }
//             }
//             Some((key, value)) => Ok(Some((key, value.bytes))),
//         }
//     }
// }

impl<'a> Iter<'a> for SSTableIter<'a> {
    type Item = KeyValue;

    fn try_next(&mut self) -> KernelResult<Option<Self::Item>> {
        match self.data_iter.try_next()? {
            None => {
                if let Some((_, index)) = self.index_iter.try_next()? {
                    self.data_iter_seek(Seek::First, index)?;

                    Ok(self
                        .data_iter
                        .try_next()?
                        .map(|(key, value)| (key, value.bytes)))
                } else {
                    Ok(None)
                }
            }
            Some((key, value)) => Ok(Some((key, value.bytes))),
        }
    }

    fn is_valid(&self) -> bool {
        self.data_iter.is_valid()
    }
}

impl<'a> SeekIter<'a> for SSTableIter<'a> {
    fn seek(&mut self, seek: Seek<'_>) -> KernelResult<()> {
        self.index_iter.seek(seek)?;

        if let Some((_, index)) = self.index_iter.try_next()? {
            self.data_iter_seek(seek, index)?;
        }
        if matches!(seek, Seek::Last) {
            self.data_iter.seek(Seek::Last)?;
        }

        Ok(())
    }
}
