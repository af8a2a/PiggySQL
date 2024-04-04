use crate::lsm::compactor::LEVEL_0;
use crate::lsm::iterator::{Iter, Seek, SeekIter};
use crate::lsm::mem_table::KeyValue;
use crate::lsm::version::Version;
use crate::KernelResult;
use crate::KernelError;

const LEVEL_0_SEEK_MESSAGE: &str = "level 0 cannot seek";

pub(crate) struct LevelIter<'a> {
    version: &'a Version,
    level: usize,
    level_len: usize,

    offset: usize,
    child_iter: Box<dyn SeekIter<'a, Item = KeyValue> + 'a + Sync + Send>,
}

impl<'a> LevelIter<'a> {
    #[allow(dead_code)]
    pub(crate) fn new(version: &'a Version, level: usize) -> KernelResult<LevelIter<'a>> {
        let table = version.table(level, 0).ok_or(KernelError::DataEmpty)?;
        let child_iter = table.iter()?;
        let level_len = version.level_len(level);

        Ok(Self {
            version,
            level,
            level_len,
            offset: 0,
            child_iter,
        })
    }

    fn child_iter_seek(&mut self, seek: Seek<'_>, offset: usize) -> KernelResult<()> {
        self.offset = offset;
        if self.is_valid() {
            if let Some(table) = self.version.table(self.level, offset) {
                self.child_iter = table.iter()?;
                self.child_iter.seek(seek)?;
            }
        }

        Ok(())
    }

    fn seek_ward(&mut self, key: &[u8], seek: Seek<'_>) -> KernelResult<()> {
        let level = self.level;

        if level == LEVEL_0 {
            return Err(KernelError::NotSupport(LEVEL_0_SEEK_MESSAGE));
        }
        self.child_iter_seek(seek, self.version.query_meet_index(key, level))
    }
}

impl<'a> Iter<'a> for LevelIter<'a> {
    type Item = KeyValue;

    fn try_next(&mut self) -> KernelResult<Option<Self::Item>> {
        match self.child_iter.try_next()? {
            None => {
                self.child_iter_seek(Seek::First, self.offset + 1)?;
                self.child_iter.try_next()
            }
            Some(item) => Ok(Some(item)),
        }
    }

    fn is_valid(&self) -> bool {
        self.offset < self.level_len
    }
}

impl<'a> SeekIter<'a> for LevelIter<'a> {
    /// Tips: Level 0的LevelIter不支持Seek
    /// 因为Level 0中的SSTable并非有序排列，其中数据范围是可能交错的
    fn seek(&mut self, seek: Seek<'_>) -> KernelResult<()> {
        match seek {
            Seek::First => self.child_iter_seek(Seek::First, 0),
            Seek::Last => self.child_iter_seek(Seek::Last, self.level_len - 1),
            Seek::Backward(key) => self.seek_ward(key, seek),
        }
    }
}