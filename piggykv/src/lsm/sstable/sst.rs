use core::slice::SlicePattern;
use std::{io::SeekFrom, sync::Arc};

use bytes::Bytes;
use parking_lot::Mutex;
use tracing::info;

use crate::{
    error::KernelError,
    io::{IoFactory, IoReader, IoType},
    lsm::{
        iterator::SeekIter,
        mem_table::KeyValue,
        sstable::sst::{
            block::{BlockBuilder, BlockOptions, CompressType, Value},
            footer::TABLE_FOOTER_SIZE,
        },
        storage::Config,
    },
    utils::bloom_filter::BloomFilter,
    KernelResult,
};

use self::{
    block::{Block, BlockCache, BlockItem, BlockType, Index, MetaBlock},
    footer::Footer,
    iter::SSTableIter,
};

use super::Table;

pub(crate) mod block;
pub(crate) mod block_iter;
mod footer;
pub(crate) mod iter;

/// SSTable
///
/// SSTable仅加载MetaBlock与Footer，避免大量冷数据时冗余的SSTable加载的空间占用
pub(crate) struct SSTable {
    // 表索引信息
    footer: Footer,
    // 文件IO操作器
    reader: Mutex<Box<dyn IoReader>>,
    // 该SSTable的唯一编号(时间递增)
    gen: i64,
    // 统计信息存储Block
    meta: MetaBlock,
    // Block缓存(Index/Value)
    cache: Arc<BlockCache>,
}

impl SSTable {
    pub(crate) async fn new(
        io_factory: &IoFactory,
        config: &Config,
        cache: Arc<BlockCache>,
        gen: i64,
        vec_data: Vec<KeyValue>,
        level: usize,
        io_type: IoType,
    ) -> KernelResult<SSTable> {
        let len = vec_data.len();
        let data_restart_interval = config.data_restart_interval;
        let index_restart_interval = config.index_restart_interval;
        let mut filter = BloomFilter::new(len, config.desired_error_prob);

        let mut builder = BlockBuilder::new(
            BlockOptions::from(config)
                .compress_type(CompressType::LZ4)
                .data_restart_interval(data_restart_interval)
                .index_restart_interval(index_restart_interval),
        );
        for data in vec_data {
            let (key, value) = data;
            filter.insert(key.as_slice());
            builder.add((key, Value::from(value)));
        }
        let meta = MetaBlock {
            filter,
            len,
            index_restart_interval,
            data_restart_interval,
        };
        let (mut bytes, data_bytes_len, index_bytes_len) = builder.build().await?;
        meta.to_raw(&mut bytes)?;

        let footer = Footer {
            level: level as u8,
            index_offset: data_bytes_len as u32,
            index_len: index_bytes_len as u32,
            meta_offset: (data_bytes_len + index_bytes_len) as u32,
            meta_len: (bytes.len() - data_bytes_len + index_bytes_len) as u32,
            size_of_disk: (bytes.len() + TABLE_FOOTER_SIZE) as u32,
        };
        footer.to_raw(&mut bytes)?;

        let mut writer = io_factory.writer(gen, io_type)?;
        writer.write_all(&bytes)?;
        writer.flush()?;
        info!("[SsTable: {}][create][MetaBlock]: {:?}", gen, meta);

        let reader = Mutex::new(io_factory.reader(gen, io_type)?);
        Ok(SSTable {
            footer,
            reader,
            gen,
            meta,
            cache,
        })
    }

    /// 通过已经存在的文件构建SSTable
    ///
    /// 使用原有的路径与分区大小恢复出一个有内容的SSTable
    pub(crate) fn load_from_file(
        mut reader: Box<dyn IoReader>,
        cache: Arc<BlockCache>,
    ) -> KernelResult<Self> {
        let gen = reader.get_gen();
        let footer = Footer::read_to_file(reader.as_mut())?;
        let Footer {
            size_of_disk,
            meta_offset,
            meta_len,
            ..
        } = &footer;
        info!(
            "[SsTable: {gen}][load_from_file][MetaBlock]: {footer:?}, Size of Disk: {}, IO Type: {:?}",
            size_of_disk ,
            reader.get_type()
        );

        let mut buf = vec![0; *meta_len as usize];
        let _ = reader.seek(SeekFrom::Start(*meta_offset as u64))?;
        let _ = reader.read(&mut buf)?;

        let meta = MetaBlock::from_raw(&buf);
        let reader = Mutex::new(reader);
        Ok(SSTable {
            footer,
            gen,
            reader,
            meta,
            cache,
        })
    }

    pub(crate) fn data_block(&self, index: Index) -> KernelResult<BlockType> {
        Ok(BlockType::Data(Self::loading_block(
            self.reader.lock().as_mut(),
            index.offset(),
            index.len(),
            CompressType::LZ4,
            self.meta.data_restart_interval,
        )?))
    }

    pub(crate) fn index_block(&self) -> KernelResult<&Block<Index>> {
        self.cache
            .get_or_insert((self.gen(), None), |_| {
                let Footer {
                    index_offset,
                    index_len,
                    ..
                } = self.footer;
                Ok(BlockType::Index(Self::loading_block(
                    self.reader.lock().as_mut(),
                    index_offset,
                    index_len as usize,
                    CompressType::None,
                    self.meta.index_restart_interval,
                )?))
            })
            .map(|block_type| match block_type {
                BlockType::Index(data_block) => Some(data_block),
                _ => None,
            })?
            .ok_or(KernelError::DataEmpty)
    }

    fn loading_block<T>(
        reader: &mut dyn IoReader,
        offset: u32,
        len: usize,
        compress_type: CompressType,
        restart_interval: usize,
    ) -> KernelResult<Block<T>>
    where
        T: BlockItem,
    {
        let mut buf = vec![0; len];
        let _ = reader.seek(SeekFrom::Start(offset as u64))?;
        reader.read_exact(&mut buf)?;

        Block::decode(buf, compress_type, restart_interval)
    }
}

impl Table for SSTable {
    fn query(&self, key: &[u8]) -> KernelResult<Option<KeyValue>> {
        if self.meta.filter.contains(key) {
            let index_block = self.index_block()?;

            if let BlockType::Data(data_block) = self.cache.get_or_insert(
                (self.gen(), Some(index_block.find_with_upper(key))),
                |(_, index)| {
                    let index = (*index).ok_or_else(|| KernelError::DataEmpty)?;
                    Self::data_block(self, index)
                },
            )? {
                if let (value, true) = data_block.find(key) {
                    return Ok(Some((Bytes::copy_from_slice(key), value)));
                }
            }
        }

        Ok(None)
    }

    fn len(&self) -> usize {
        self.meta.len
    }

    fn size_of_disk(&self) -> u64 {
        self.footer.size_of_disk as u64
    }

    fn gen(&self) -> i64 {
        self.gen
    }

    fn level(&self) -> usize {
        self.footer.level as usize
    }

    fn iter<'a>(
        &'a self,
    ) -> KernelResult<Box<dyn SeekIter<'a, Item = KeyValue> + 'a + Send + Sync>> {
        Ok(SSTableIter::new(self).map(Box::new)?)
    }
}
