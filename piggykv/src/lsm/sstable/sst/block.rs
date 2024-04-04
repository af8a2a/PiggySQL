
use std::io::{Cursor, Read, Write};

use crate::lsm::storage::Config;
use crate::utils::bloom_filter::BloomFilter;
use crate::utils::lru_cache::ShardingLruCache;
use crate::KernelError;
use bytes::{Buf, BufMut, Bytes};
use integer_encoding::{FixedInt, FixedIntWriter, VarIntReader, VarIntWriter};
use itertools::Itertools;
use lz4::Decoder;
use std::cmp::min;
use std::mem;

use crate::KernelResult;

pub(crate) type BlockCache = ShardingLruCache<(i64, Option<Index>), BlockType>;

pub(crate) const DEFAULT_BLOCK_SIZE: usize = 4 * 1024;

/// 不动态决定Restart是因为Restart的范围固定可以做到更简单的Entry二分查询，提高性能
pub(crate) const DEFAULT_DATA_RESTART_INTERVAL: usize = 16;

pub(crate) const DEFAULT_INDEX_RESTART_INTERVAL: usize = 2;

const CRC_SIZE: usize = 4;

pub(crate) type BlockKeyValue<T> = (Bytes, T);
#[derive(Debug, PartialEq, Eq, Clone,Hash)]
pub(crate) enum BlockType {
    Data(Block<Value>),
    Index(Block<Index>),
}

#[derive(Debug,Hash, PartialEq, Eq, Clone)]
pub(crate) struct Entry<T> {
    unshared_len: usize,
    shared_len: usize,
    pub(crate) key: Bytes,
    pub(crate) item: T,
}

impl<T> Entry<T>
where
    T: BlockItem,
{
    pub(crate) fn new(shared_len: usize, unshared_len: usize, key: Bytes, item: T) -> Self {
        Entry {
            unshared_len,
            shared_len,
            key,
            item,
        }
    }

    pub(crate) fn encode(&self, bytes: &mut Vec<u8>) -> KernelResult<()> {
        bytes.write_varint(self.unshared_len as u32)?;
        bytes.write_varint(self.shared_len as u32)?;
        bytes.write_all(&self.key)?;
        self.item.encode(bytes)?;

        Ok(())
    }

    pub(crate) fn batch_decode(cursor: &mut Cursor<Vec<u8>>) -> KernelResult<Vec<(usize, Self)>> {
        let mut vec_entry = Vec::new();
        let mut index = 0;

        while !cursor.is_empty() {
            vec_entry.push((index, Self::decode(cursor)?));
            index += 1;
        }

        Ok(vec_entry)
    }

    pub(crate) fn decode<R: Read>(reader: &mut R) -> KernelResult<Entry<T>> {
        let unshared_len = reader.read_varint::<u32>()? as usize;
        let shared_len = reader.read_varint::<u32>()? as usize;

        let mut bytes = vec![0u8; unshared_len];
        let _ = reader.read(&mut bytes)?;

        Ok(Self {
            unshared_len,
            shared_len,
            key: Bytes::from(bytes),
            item: T::decode(reader)?,
        })
    }
}

/// 键值对对应的Value
#[derive(Debug,Hash, PartialEq, Eq, Clone)]
pub(crate) struct Value {
    value_len: usize,
    pub(crate) bytes: Option<Bytes>,
}

impl From<Option<Bytes>> for Value {
    fn from(bytes: Option<Bytes>) -> Self {
        let value_len = bytes.as_ref().map_or(0, Bytes::len);
        Value { value_len, bytes }
    }
}

/// Block索引
#[derive(Debug, PartialEq, Eq, Copy, Clone, Hash)]
pub(crate) struct Index {
    offset: u32,
    len: usize,
}

impl Index {
    fn new(offset: u32, len: usize) -> Self {
        Index { offset, len }
    }

    pub(crate) fn offset(&self) -> u32 {
        self.offset
    }

    pub(crate) fn len(&self) -> usize {
        self.len
    }
}

pub(crate) trait BlockItem: Sized + Clone {
    /// 由于需要直接连续序列化，因此使用Read进行Bytes读取
    fn decode<T>(reader: &mut T) -> KernelResult<Self>
    where
        T: Read + ?Sized;

    fn encode(&self, bytes: &mut Vec<u8>) -> KernelResult<()>;
}

impl BlockItem for Value {
    fn decode<T>(mut reader: &mut T) -> KernelResult<Self>
    where
        T: Read + ?Sized,
    {
        let value_len = reader.read_varint::<u32>()? as usize;

        let bytes = (value_len > 0)
            .then(|| {
                let mut value = vec![0u8; value_len];
                reader.read(&mut value).ok().map(|_| Bytes::from(value))
            })
            .flatten();

        Ok(Value { value_len, bytes })
    }

    fn encode(&self, bytes: &mut Vec<u8>) -> KernelResult<()> {
        bytes.write_varint(self.value_len as u32)?;

        if let Some(value) = &self.bytes {
            bytes.write_all(value)?;
        }
        Ok(())
    }
}

impl BlockItem for Index {
    fn decode<T>(mut reader: &mut T) -> KernelResult<Self>
    where
        T: Read + ?Sized,
    {
        let offset = reader.read_varint::<u32>()?;
        let len = reader.read_varint::<u32>()? as usize;

        Ok(Index { offset, len })
    }

    fn encode(&self, bytes: &mut Vec<u8>) -> KernelResult<()> {
        bytes.write_varint(self.offset)?;
        bytes.write_varint(self.len as u32)?;

        Ok(())
    }
}

#[derive(Clone, Copy)]
pub(crate) enum CompressType {
    None,
    LZ4,
}

#[derive(Debug)]
pub(crate) struct MetaBlock {
    pub(crate) filter: BloomFilter<[u8]>,
    pub(crate) len: usize,
    pub(crate) index_restart_interval: usize,
    pub(crate) data_restart_interval: usize,
}

impl MetaBlock {
    pub(crate) fn to_raw(&self, bytes: &mut Vec<u8>) -> KernelResult<()> {
        bytes.write_fixedint(self.len as u32)?;
        bytes.write_fixedint(self.index_restart_interval as u32)?;
        bytes.write_fixedint(self.data_restart_interval as u32)?;

        self.filter.to_raw(bytes)?;

        Ok(())
    }

    pub(crate) fn from_raw(bytes: &[u8]) -> Self {
        let len = u32::decode_fixed(&bytes[0..4]) as usize;
        let index_restart_interval = u32::decode_fixed(&bytes[4..8]) as usize;
        let data_restart_interval = u32::decode_fixed(&bytes[8..12]) as usize;
        let filter = BloomFilter::from_raw(&bytes[12..]);

        Self {
            filter,
            len,
            index_restart_interval,
            data_restart_interval,
        }
    }
}

/// Block SSTable最小的存储单位
///
/// 分为DataBlock和IndexBlock
#[derive(Debug, PartialEq,Hash, Eq, Clone)]
pub(crate) struct Block<T> {
    restart_interval: usize,
    vec_entry: Vec<(usize, Entry<T>)>,
}

#[derive(Clone)]
pub(crate) struct BlockOptions {
    block_size: usize,
    compress_type: CompressType,
    data_restart_interval: usize,
    index_restart_interval: usize,
}

impl From<&Config> for BlockOptions {
    fn from(config: &Config) -> Self {
        BlockOptions {
            block_size: config.block_size,
            compress_type: CompressType::None,
            data_restart_interval: config.data_restart_interval,
            index_restart_interval: config.index_restart_interval,
        }
    }
}

impl BlockOptions {
    #[allow(dead_code)]
    pub(crate) fn new() -> Self {
        BlockOptions {
            block_size: DEFAULT_BLOCK_SIZE,
            compress_type: CompressType::None,
            data_restart_interval: DEFAULT_DATA_RESTART_INTERVAL,
            index_restart_interval: DEFAULT_INDEX_RESTART_INTERVAL,
        }
    }
    #[allow(dead_code)]
    pub(crate) fn block_size(mut self, block_size: usize) -> Self {
        self.block_size = block_size;
        self
    }
    #[allow(dead_code)]
    pub(crate) fn compress_type(mut self, compress_type: CompressType) -> Self {
        self.compress_type = compress_type;
        self
    }
    #[allow(dead_code)]
    pub(crate) fn data_restart_interval(mut self, data_restart_interval: usize) -> Self {
        self.data_restart_interval = data_restart_interval;
        self
    }
    #[allow(dead_code)]
    pub(crate) fn index_restart_interval(mut self, index_restart_interval: usize) -> Self {
        self.index_restart_interval = index_restart_interval;
        self
    }
}

struct BlockBuf {
    bytes_size: usize,
    vec_key_value: Vec<BlockKeyValue<Value>>,
}

impl BlockBuf {
    fn new() -> Self {
        BlockBuf {
            bytes_size: 0,
            vec_key_value: Vec::new(),
        }
    }

    fn add(&mut self, key_value: BlockKeyValue<Value>) {
        // 断言新插入的键值对的Key大于buf中最后的key
        if let Some(last_key) = self.last_key() {
            assert!(last_key.cmp(&key_value.0).is_lt());
        }
        self.bytes_size += key_value_bytes_len(&key_value);
        self.vec_key_value.push(key_value);
    }

    /// 获取最后一个Key
    fn last_key(&self) -> Option<&Bytes> {
        self.vec_key_value.last().map(|key_value| &key_value.0)
    }

    /// 刷新且弹出其缓存的键值对与其中last_key
    fn flush(&mut self) -> (Vec<BlockKeyValue<Value>>, Option<Bytes>) {
        self.bytes_size = 0;
        let last_key = self.last_key().cloned();

        (mem::take(&mut self.vec_key_value), last_key)
    }
}

/// Block构建器
///
/// 请注意add时
pub(crate) struct BlockBuilder {
    options: BlockOptions,
    len: usize,
    buf: BlockBuf,
    vec_block: Vec<(Block<Value>, Bytes)>,
}

/// 获取键值对得到其空间占用数
fn key_value_bytes_len(key_value: &BlockKeyValue<Value>) -> usize {
    let (key, value) = key_value;
    key.len() + value.bytes.as_ref().map_or(0, Bytes::len)
}

impl BlockBuilder {
    pub(crate) fn new(options: BlockOptions) -> Self {
        BlockBuilder {
            options,
            len: 0,
            buf: BlockBuf::new(),
            vec_block: Vec::new(),
        }
    }

    /// 查看已参与构建的键值对数量
    #[allow(dead_code)]
    pub(crate) fn len(&self) -> usize {
        self.len
    }

    /// 插入需要构建为Block的键值对
    ///
    /// 请注意add的键值对需要自行保证key顺序插入,否则可能会出现问题
    pub(crate) fn add(&mut self, key_value: BlockKeyValue<Value>) {
        self.buf.add(key_value);
        self.len += 1;
        // 超过指定的Block大小后进行Block构建(默认为4K大小)
        if self.is_out_of_byte() {
            self._build();
        }
    }

    fn is_out_of_byte(&self) -> bool {
        self.buf.bytes_size >= self.options.block_size
    }

    /// 封装用的构建Block方法
    ///
    /// 刷新buf获取其中的所有键值对与其中最大的key进行前缀压缩构建为Block
    fn _build(&mut self) {
        if let (vec_kv, Some(last_key)) = self.buf.flush() {
            self.vec_block.push((
                Block::new(vec_kv, self.options.data_restart_interval),
                last_key,
            ));
        }
    }

    /// 构建多个Block连续序列化组合成的两个Bytes 前者为多个DataBlock，后者为单个IndexBlock
    pub(crate) async fn build(mut self) -> KernelResult<(Vec<u8>, usize, usize)> {
        self._build();

        let mut blocks_bytes = vec![];
        let mut offset = 0u32;

        let mut indexes = Vec::with_capacity(self.vec_block.len());

        for (block, last_key) in self.vec_block {
            block.encode(self.options.compress_type, &mut blocks_bytes)?;

            let len = blocks_bytes.len() - offset as usize;

            indexes.push((last_key, Index::new(offset, len)));
            offset += len as u32;
        }
        let data_bytes_len = blocks_bytes.len();

        Block::new(indexes, self.options.index_restart_interval)
            .encode(CompressType::None, &mut blocks_bytes)?;
        let index_bytes_len = blocks_bytes.len() - data_bytes_len;

        Ok((blocks_bytes, data_bytes_len, index_bytes_len))
    }
}

impl Block<Value> {
    /// 通过Key查询对应Value
    ///
    /// 返回数据为Value的Option以及是否存在
    pub(crate) fn find(&self, key: &[u8]) -> (Option<Bytes>, bool) {
        self.binary_search(key)
            .ok()
            .and_then(|index| {
                self.vec_entry
                    .get(index)
                    .map(|(_, entry)| (entry.item.bytes.clone(), true))
            })
            .unwrap_or((None, false))
    }
}

impl<T> Block<T> {
    #[allow(dead_code)]
    pub(crate) fn entry_len(&self) -> usize {
        self.vec_entry.len()
    }

    /// 获取该Entry对应的shared_key前缀
    ///
    /// 具体原理是通过被固定的restart_interval进行前缀压缩的Block，
    /// 通过index获取前方最近的Restart，得到的Key通过shared_len进行截取以此得到shared_key
    pub(crate) fn shared_key_prefix(&self, index: usize, shared_len: usize) -> &[u8] {
        &self.vec_entry[index - index % self.restart_interval].1.key[0..shared_len]
    }

    pub(crate) fn restart_interval(&self) -> usize {
        self.restart_interval
    }

    /// 获取指定index的entry
    pub(crate) fn get_entry(&self, index: usize) -> &Entry<T> {
        &self.vec_entry[index].1
    }

    /// 获取指定index所属restart的shared_len
    ///
    /// 当index为restart_entry时也会获取其区域内的其他节点相同的shared_len
    pub(crate) fn restart_shared_len(&self, index: usize) -> usize {
        if index % self.restart_interval != 0 {
            self.get_entry(index).shared_len
        } else {
            self.vec_entry
                .get(index + 1)
                .map_or(0, |(_, entry)| entry.shared_len)
        }
    }
}

impl<T> Block<T>
where
    T: BlockItem,
{
    /// 新建Block，同时Block会进行前缀压缩
    pub(crate) fn new(vec_kv: Vec<BlockKeyValue<T>>, restart_interval: usize) -> Block<T> {
        let vec_sharding_len = sharding_shared_len(&vec_kv, restart_interval);
        let vec_entry = vec_kv
            .into_iter()
            .enumerate()
            .map(|(index, (key, item))| {
                let shared_len = if index % restart_interval == 0 {
                    0
                } else {
                    vec_sharding_len[index / restart_interval]
                };
                (
                    index,
                    Entry::new(
                        shared_len,
                        key.len() - shared_len,
                        Bytes::copy_from_slice(&key[shared_len..]),
                        item,
                    ),
                )
            })
            .collect_vec();
        Block {
            restart_interval,
            vec_entry,
        }
    }

    /// 查询相等或最近较大的Key
    pub(crate) fn find_with_upper(&self, key: &[u8]) -> T {
        let entries_len = self.vec_entry.len();
        let index = self
            .binary_search(key)
            .unwrap_or_else(|index| min(entries_len - 1, index));
        self.vec_entry[index].1.item.clone()
    }

    pub(crate) fn binary_search(&self, key: &[u8]) -> Result<usize, usize> {
        self.vec_entry.binary_search_by(|(index, entry)| {
            if entry.shared_len > 0 {
                // 对有前缀压缩的Key进行前缀拼接
                let shared_len = min(entry.shared_len, key.len());
                key[0..shared_len]
                    .cmp(self.shared_key_prefix(*index, shared_len))
                    .then_with(|| key[shared_len..].cmp(&entry.key))
            } else {
                key.cmp(&entry.key)
            }
            .reverse()
        })
    }

    /// 序列化后进行压缩
    ///
    /// 可选LZ4与不压缩
    pub(crate) fn encode(
        &self,
        compress_type: CompressType,
        bytes: &mut Vec<u8>,
    ) -> KernelResult<()> {
        match compress_type {
            CompressType::None => self.to_raw(bytes)?,
            CompressType::LZ4 => {
                let mut buf = Vec::new();
                self.to_raw(&mut buf)?;

                let mut encoder = lz4::EncoderBuilder::new().level(4).build(bytes.writer())?;
                let _ = encoder.write(&buf[..])?;
                let (_, result) = encoder.finish();

                result?;
            }
        }

        Ok(())
    }

    /// 解压后反序列化
    ///
    /// 与encode对应，进行数据解压操作并反序列化为Block
    pub(crate) fn decode(
        buf: Vec<u8>,
        compress_type: CompressType,
        restart_interval: usize,
    ) -> KernelResult<Self> {
        let buf = match compress_type {
            CompressType::None => buf,
            CompressType::LZ4 => {
                let mut decoder = Decoder::new(buf.reader())?;
                let mut decoded = Vec::with_capacity(DEFAULT_BLOCK_SIZE);
                let _ = decoder.read_to_end(&mut decoded)?;
                decoded
            }
        };
        Self::from_raw(buf, restart_interval)
    }

    /// 读取Bytes进行Block的反序列化
    pub(crate) fn from_raw(mut buf: Vec<u8>, restart_interval: usize) -> KernelResult<Self> {
        assert!(!buf.is_empty());
        let date_bytes_len = buf.len() - CRC_SIZE;
        if crc32fast::hash(&buf) == u32::decode_fixed(&buf[date_bytes_len..]) {
            return Err(KernelError::CrcMisMatch);
        }
        buf.truncate(date_bytes_len);

        let mut cursor = Cursor::new(buf);
        let vec_entry = Entry::<T>::batch_decode(&mut cursor)?;
        Ok(Self {
            restart_interval,
            vec_entry,
        })
    }

    /// 序列化该Block
    ///
    /// 与from_raw对应，序列化时会生成crc_code用于反序列化时校验
    pub(crate) fn to_raw(&self, bytes: &mut Vec<u8>) -> KernelResult<()> {
        let start = bytes.len();
        for (_, entry) in &self.vec_entry {
            entry.encode(bytes)?;
        }
        bytes.append(&mut crc32fast::hash(&bytes[start..]).encode_fixed_vec());

        Ok(())
    }
}

/// 批量以restart_interval进行shared_len的获取
fn sharding_shared_len<T>(vec_kv: &[BlockKeyValue<T>], restart_interval: usize) -> Vec<usize>
where
    T: BlockItem,
{
    let mut vec_shared_key =
        Vec::with_capacity((vec_kv.len() + restart_interval - 1) / restart_interval);
    for (_, group) in &vec_kv
        .iter()
        .enumerate()
        .group_by(|(i, _)| i / restart_interval)
    {
        vec_shared_key.push(longest_shared_len(
            group.map(|(_, item)| item).collect_vec(),
        ))
    }
    vec_shared_key
}

/// 查询一组KV的Key最长前缀计数
fn longest_shared_len<T>(sharding: Vec<&BlockKeyValue<T>>) -> usize {
    if sharding.is_empty() {
        return 0;
    }
    let mut min_len = usize::MAX;
    for kv in &sharding {
        min_len = min(min_len, kv.0.len());
    }
    let mut low = 0;
    let mut high = min_len;
    while low < high {
        let mid = (high - low + 1) / 2 + low;
        if is_common_prefix(&sharding, mid) {
            low = mid;
        } else {
            high = mid - 1;
        }
    }
    return low;

    fn is_common_prefix<T>(sharding: &[&BlockKeyValue<T>], len: usize) -> bool {
        let first = sharding[0];
        for kv in sharding.iter().skip(1) {
            for i in 0..len {
                if first.0[i] != kv.0[i] {
                    return false;
                }
            }
        }
        true
    }
}
