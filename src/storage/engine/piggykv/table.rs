pub(crate) mod bloom;
mod builder;
mod iterator;
use crate::errors::Result;

use std::fs::File;
use std::io::Write;
use std::path::Path;
use std::sync::Arc;

pub use builder::SsTableBuilder;
use bytes::{Buf, BufMut};
pub use iterator::SsTableIterator;
use tracing::error;

use self::bloom::Bloom;

use super::{
    block::Block,
    key::{KeyBytes, KeySlice},
    lsm_storage::BlockCache,
};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BlockMeta {
    /// Offset of this data block.
    pub offset: usize,
    /// The first key of the data block.
    pub first_key: KeyBytes,
    /// The last key of the data block.
    pub last_key: KeyBytes,
}

impl BlockMeta {
    /// Encode block meta to a buffer.
    pub fn encode_block_meta(block_meta: &[BlockMeta], max_ts: u64, buf: &mut Vec<u8>) {
        let mut estimated_size = std::mem::size_of::<u32>(); // number of blocks
        for meta in block_meta {
            // The size of offset
            estimated_size += std::mem::size_of::<u32>();
            // The size of key length
            estimated_size += std::mem::size_of::<u16>();
            // The size of actual key
            estimated_size += meta.first_key.raw_len();
            // The size of key length
            estimated_size += std::mem::size_of::<u16>();
            // The size of actual key
            estimated_size += meta.last_key.raw_len();
        }
        estimated_size += std::mem::size_of::<u64>(); // max timestamp
        estimated_size += std::mem::size_of::<u32>(); // checksum

        // Reserve the space to improve performance, especially when the size of incoming data is
        // large
        buf.reserve(estimated_size);
        let original_len = buf.len();
        buf.put_u32(block_meta.len() as u32);
        for meta in block_meta {
            buf.put_u32(meta.offset as u32);
            buf.put_u16(meta.first_key.key_len() as u16);
            buf.put_slice(meta.first_key.key_ref());
            buf.put_u64(meta.first_key.ts());
            buf.put_u16(meta.last_key.key_len() as u16);
            buf.put_slice(meta.last_key.key_ref());
            buf.put_u64(meta.last_key.ts());
        }
        buf.put_u64(max_ts);

        buf.put_u32(crc32fast::hash(&buf[original_len + 4..]));
        assert_eq!(estimated_size, buf.len() - original_len);
    }

    /// Decode block meta from a buffer.
    pub fn decode_block_meta(mut buf: &[u8]) -> Result<(Vec<BlockMeta>, u64)> {
        let mut block_meta = Vec::new();
        let num = buf.get_u32() as usize;
        let checksum = crc32fast::hash(&buf[..buf.remaining() - 4]);
        for _ in 0..num {
            let offset = buf.get_u32() as usize;
            let first_key_len = buf.get_u16() as usize;
            let first_key =
                KeyBytes::from_bytes_with_ts(buf.copy_to_bytes(first_key_len), buf.get_u64());
            let last_key_len: usize = buf.get_u16() as usize;
            let last_key =
                KeyBytes::from_bytes_with_ts(buf.copy_to_bytes(last_key_len), buf.get_u64());
            block_meta.push(BlockMeta {
                offset,
                first_key,
                last_key,
            });
        }
        let max_ts = buf.get_u64();

        if buf.get_u32() != checksum {
            error!("checksum mismatched!");
            panic!("meta checksum mismatched");
        }

        Ok((block_meta, max_ts))
    }
}

/// A file object.
pub struct FileObject(Option<File>, u64);

impl FileObject {
    pub fn read(&self, offset: u64, len: u64) -> Result<Vec<u8>> {
        let mut data = vec![0; len as usize];

        #[cfg(windows)]
        {
            use std::os::windows::fs::FileExt;
            self.0.as_ref().unwrap().seek_read(&mut data[..], offset)?;
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::FileExt;
            self.0
                .as_ref()
                .unwrap()
                .read_exact_at(&mut data[..], offset)?;
        }
        Ok(data)
    }

    pub fn size(&self) -> u64 {
        self.1
    }

    /// Create a new file object (day 2) and write the file to the disk (day 4).
    pub fn create(path: &Path, data: Vec<u8>) -> Result<Self> {
        // std::fs::write(path, &data)?;
        // println!("reach");
        let mut file = File::options()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path)
            .unwrap();
        file.write_all(&data)?;
        file.sync_all().unwrap();
        Ok(FileObject(Some(file), data.len() as u64))
    }

    pub fn open(path: &Path) -> Result<Self> {
        let file = File::options().read(true).write(true).open(path)?;
        let size = file.metadata()?.len();
        Ok(FileObject(Some(file), size))
    }
}

/// An SSTable.
pub struct SsTable {
    /// The actual storage unit of SsTable, the format is as above.
    pub(crate) file: FileObject,
    /// The meta blocks that hold info for data blocks.
    pub(crate) block_meta: Vec<BlockMeta>,
    /// The offset that indicates the start point of meta blocks in `file`.
    pub(crate) block_meta_offset: usize,
    id: usize,
    block_cache: Option<Arc<BlockCache>>,
    first_key: KeyBytes,
    last_key: KeyBytes,
    pub(crate) bloom: Option<Bloom>,
    max_ts: u64,

}
impl SsTable {
    pub fn sync(&self) -> Result<()> {
        self.file.0.as_ref().unwrap().sync_all()?;
        Ok(())
    }
    /// Open SSTable from a file.
    pub fn open(id: usize, block_cache: Option<Arc<BlockCache>>, file: FileObject) -> Result<Self> {
        let len = file.size();
        let raw_bloom_offset = file.read(len - 4, 4)?;
        let bloom_offset = (&raw_bloom_offset[..]).get_u32() as u64;
        let raw_bloom = file.read(bloom_offset, len - 4 - bloom_offset)?;
        let bloom_filter = Bloom::decode(&raw_bloom)?;
        let raw_meta_offset = file.read(bloom_offset - 4, 4)?;
        let block_meta_offset = (&raw_meta_offset[..]).get_u32() as u64;
        let raw_meta = file.read(block_meta_offset, bloom_offset - 4 - block_meta_offset)?;
        let (block_meta, max_ts) = BlockMeta::decode_block_meta(&raw_meta[..])?;
        Ok(Self {
            file,
            first_key: block_meta.first().unwrap().first_key.clone(),
            last_key: block_meta.last().unwrap().last_key.clone(),
            block_meta,
            block_meta_offset: block_meta_offset as usize,
            id,
            block_cache,
            bloom: Some(bloom_filter),
            max_ts,
        })
    }

    /// Create a mock SST with only first key + last key metadata
    pub fn create_meta_only(
        id: usize,
        file_size: u64,
        first_key: KeyBytes,
        last_key: KeyBytes,
    ) -> Self {
        Self {
            file: FileObject(None, file_size),
            block_meta: vec![],
            block_meta_offset: 0,
            id,
            block_cache: None,
            first_key,
            last_key,
            bloom: None,
            max_ts:0,
        }
    }

    /// Read a block from the disk.
    pub fn read_block(&self, block_idx: usize) -> Result<Arc<Block>> {
        let offset = self.block_meta[block_idx].offset;
        let offset_end = self
            .block_meta
            .get(block_idx + 1)
            .map_or(self.block_meta_offset, |x| x.offset);
        let block_len = offset_end - offset - 4;
        let block_data_with_chksum: Vec<u8> = self
            .file
            .read(offset as u64, (offset_end - offset) as u64)?;
        let block_data = &block_data_with_chksum[..block_len];
        let checksum = (&block_data_with_chksum[block_len..]).get_u32();
        if checksum != crc32fast::hash(block_data) {
            error!("block checksum mismatched!");
            panic!("block checksum mismatched");
        }
        Ok(Arc::new(Block::decode(block_data)))
    }

    /// Read a block from disk, with block cache.
    pub fn read_block_cached(&self, block_idx: usize) -> Result<Arc<Block>> {
        if let Some(ref block_cache) = self.block_cache {
            let blk = block_cache
                .try_get_with((self.id, block_idx), || self.read_block(block_idx))
                .unwrap();
            Ok(blk)
        } else {
            self.read_block(block_idx)
        }
    }

    /// Find the block that may contain `key`.
    pub fn find_block_idx(&self, key: KeySlice) -> usize {
        self.block_meta
            .partition_point(|meta| meta.first_key.as_key_slice() <= key)
            .saturating_sub(1)
    }

    /// Get number of data blocks.
    pub fn num_of_blocks(&self) -> usize {
        self.block_meta.len()
    }

    pub fn first_key(&self) -> &KeyBytes {
        &self.first_key
    }

    pub fn last_key(&self) -> &KeyBytes {
        &self.last_key
    }

    pub fn table_size(&self) -> u64 {
        self.file.1
    }

    pub fn sst_id(&self) -> usize {
        self.id
    }
    
    pub fn max_ts(&self) -> u64 {
        self.max_ts
    }
    #[cfg(test)]
    pub(crate) fn open_for_test(file: FileObject) -> Result<Self> {
        Self::open(0, None, file)
    }

}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use bytes::Bytes;

    use crate::storage::engine::piggykv::{iterators::StorageIterator, key::{KeySlice, KeyVec}};
    use tempfile::{tempdir, TempDir};
    use super::{SsTable, SsTableBuilder, SsTableIterator};


    #[test]
    fn test_sst_build_single_key() {
        let mut builder = SsTableBuilder::new(16);
        builder.add(KeySlice::for_testing_from_slice_no_ts(b"233"), b"233333");
        let dir = tempdir().unwrap();
        builder.build_for_test(dir.path().join("1.sst")).unwrap();
    }
    
    #[test]
    fn test_sst_build_two_blocks() {
        let mut builder = SsTableBuilder::new(16);
        builder.add(KeySlice::for_testing_from_slice_no_ts(b"11"), b"11");
        builder.add(KeySlice::for_testing_from_slice_no_ts(b"22"), b"22");
        builder.add(KeySlice::for_testing_from_slice_no_ts(b"33"), b"11");
        builder.add(KeySlice::for_testing_from_slice_no_ts(b"44"), b"22");
        builder.add(KeySlice::for_testing_from_slice_no_ts(b"55"), b"11");
        builder.add(KeySlice::for_testing_from_slice_no_ts(b"66"), b"22");
        assert!(builder.meta.len() >= 2);
        let dir = tempdir().unwrap();
        builder.build_for_test(dir.path().join("1.sst")).unwrap();
    }
    
    fn key_of(idx: usize) -> KeyVec {
        KeyVec::for_testing_from_vec_no_ts(format!("key_{:03}", idx * 5).into_bytes())
    }
    
    fn value_of(idx: usize) -> Vec<u8> {
        format!("value_{:010}", idx).into_bytes()
    }
    
    fn num_of_keys() -> usize {
        100
    }
    
    fn generate_sst() -> (TempDir, SsTable) {
        let mut builder = SsTableBuilder::new(128);
        for idx in 0..num_of_keys() {
            let key = key_of(idx);
            let value = value_of(idx);
            builder.add(key.as_key_slice(), &value[..]);
        }
        let dir = tempdir().unwrap();
        let path = dir.path().join("1.sst");
        (dir, builder.build_for_test(path).unwrap())
    }
    
    #[test]
    fn test_sst_build_all() {
        generate_sst();
    }
    
    #[test]
    fn test_sst_decode() {
        let (_dir, sst) = generate_sst();
        let meta = sst.block_meta.clone();
        let new_sst = SsTable::open_for_test(sst.file).unwrap();
        assert_eq!(new_sst.block_meta, meta);
        assert_eq!(
            new_sst.first_key().for_testing_key_ref(),
            key_of(0).for_testing_key_ref()
        );
        assert_eq!(
            new_sst.last_key().for_testing_key_ref(),
            key_of(num_of_keys() - 1).for_testing_key_ref()
        );
    }
    
    fn as_bytes(x: &[u8]) -> Bytes {
        Bytes::copy_from_slice(x)
    }
    
    #[test]
    fn test_sst_iterator() {
        let (_dir, sst) = generate_sst();
        let sst = Arc::new(sst);
        let mut iter = SsTableIterator::create_and_seek_to_first(sst).unwrap();
        for _ in 0..5 {
            for i in 0..num_of_keys() {
                let key = iter.key();
                let value = iter.value();
                assert_eq!(
                    key.for_testing_key_ref(),
                    key_of(i).for_testing_key_ref(),
                    "expected key: {:?}, actual key: {:?}",
                    as_bytes(key_of(i).for_testing_key_ref()),
                    as_bytes(key.for_testing_key_ref())
                );
                assert_eq!(
                    value,
                    value_of(i),
                    "expected value: {:?}, actual value: {:?}",
                    as_bytes(&value_of(i)),
                    as_bytes(value)
                );
                iter._next().unwrap();
            }
            iter.seek_to_first().unwrap();
        }
    }
    
    #[test]
    fn test_sst_seek_key() {
        let (_dir, sst) = generate_sst();
        let sst = Arc::new(sst);
        let mut iter = SsTableIterator::create_and_seek_to_key(sst, key_of(0).as_key_slice()).unwrap();
        for offset in 1..=5 {
            for i in 0..num_of_keys() {
                let key = iter.key();
                let value = iter.value();
                assert_eq!(
                    key.for_testing_key_ref(),
                    key_of(i).for_testing_key_ref(),
                    "expected key: {:?}, actual key: {:?}",
                    as_bytes(key_of(i).for_testing_key_ref()),
                    as_bytes(key.for_testing_key_ref())
                );
                assert_eq!(
                    value,
                    value_of(i),
                    "expected value: {:?}, actual value: {:?}",
                    as_bytes(&value_of(i)),
                    as_bytes(value)
                );
                iter.seek_to_key(KeySlice::for_testing_from_slice_no_ts(
                    &format!("key_{:03}", i * 5 + offset).into_bytes(),
                ))
                .unwrap();
            }
            iter.seek_to_key(KeySlice::for_testing_from_slice_no_ts(b"k"))
                .unwrap();
        }
    }
    
}
