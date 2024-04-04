use crate::io::{FileExtension, IoFactory, IoType, IoWriter};
use crate::{KernelError, KernelResult};
use integer_encoding::FixedInt;
use std::cmp::min;
/// dermesser/leveldb-rs crates.io: v1.0.6
/// https://github.com/dermesser/leveldb-rs/blob/master/src/log.rs
/// The MIT License (MIT)
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;
use std::sync::Arc;

use super::sorted_gen_list;
use super::storage::Gen;

const BLOCK_SIZE: usize = 32 * 1024;
const HEADER_SIZE: usize = 4 + 4 + 1;

pub(crate) const DEFAULT_WAL_PATH: &str = "wal";

#[derive(Clone)]
pub(crate) struct LogLoader {
    factory: Arc<IoFactory>,
    io_type: IoType,
}

impl LogLoader {
    pub(crate) fn reload<F, R>(
        wal_dir_path: &Path,
        path_name: (&str, Option<i64>),
        io_type: IoType,
        records: &mut Vec<R>,
        fn_decode: F,
    ) -> KernelResult<(Self, i64)>
    where
        F: Fn(&mut Vec<u8>, &mut Vec<R>) -> KernelResult<()>,
    {
        let (loader, log_gen) = Self::_reload(wal_dir_path, path_name, io_type)?;
        loader.load(log_gen, records, fn_decode)?;

        Ok((loader, log_gen))
    }

    fn _reload(
        wal_dir_path: &Path,
        path_name: (&str, Option<i64>),
        io_type: IoType,
    ) -> KernelResult<(Self, i64)> {
        let (path, name) = path_name;
        let wal_path = wal_dir_path.join(path);

        let factory = Arc::new(IoFactory::new(wal_path.clone(), FileExtension::Log)?);

        let current_gen = name
            .or_else(|| {
                sorted_gen_list(&wal_path, FileExtension::Log)
                    .ok()
                    .and_then(|vec| vec.last().cloned())
            })
            .unwrap_or(Gen::create());

        Ok((LogLoader { factory, io_type }, current_gen))
    }

    /// 通过Gen载入数据进行读取
    pub(crate) fn load<F, R>(
        &self,
        gen: i64,
        records: &mut Vec<R>,
        fn_decode: F,
    ) -> KernelResult<()>
    where
        F: Fn(&mut Vec<u8>, &mut Vec<R>) -> KernelResult<()>,
    {
        let mut reader = LogReader::new(self.factory.reader(gen, self.io_type)?);
        let mut buf = vec![0; 128];

        // 当数据排列有误时仅恢复已正常读取的数据
        while reader.read(&mut buf).unwrap_or(0) > 0 {
            fn_decode(&mut buf, records)?;
        }

        Ok(())
    }

    #[allow(dead_code)]
    pub(crate) fn clean(&self, gen: i64) -> KernelResult<()> {
        self.factory.clean(gen)
    }

    pub(crate) fn writer(&self, gen: i64) -> KernelResult<LogWriter<Box<dyn IoWriter>>> {
        let new_fs = self.factory.writer(gen, self.io_type)?;
        Ok(LogWriter::new(new_fs))
    }
}

#[derive(Debug, Clone, Copy)]
pub(crate) enum RecordType {
    Full = 1,
    First = 2,
    Middle = 3,
    Last = 4,
}

impl From<u8> for RecordType {
    fn from(value: u8) -> Self {
        match value {
            1 => RecordType::Full,
            2 => RecordType::First,
            3 => RecordType::Middle,
            4 => RecordType::Last,
            _ => panic!("Unknown value: {value}"),
        }
    }
}

pub(crate) struct LogWriter<W: Write + Seek> {
    dst: W,
    current_block_offset: usize,
    block_size: usize,
}

impl<W: Write + Seek> LogWriter<W> {
    pub(crate) fn new(writer: W) -> LogWriter<W> {
        LogWriter {
            dst: writer,
            current_block_offset: 0,
            block_size: BLOCK_SIZE,
        }
    }

    /// new_with_off opens a writer starting at some offset of an existing log file. The file must
    /// have the default block size.
    #[allow(dead_code)]
    pub(crate) fn new_with_off(writer: W, off: usize) -> LogWriter<W> {
        let mut w = LogWriter::new(writer);
        w.current_block_offset = off % BLOCK_SIZE;
        w
    }

    pub(crate) fn seek_end(&mut self) -> KernelResult<u64> {
        Ok(self.dst.seek(SeekFrom::End(0))?)
    }

    pub(crate) fn add_record(&mut self, r: &[u8]) -> KernelResult<usize> {
        let mut record = r;
        let mut len = 0;

        while !record.is_empty() {
            assert!(self.block_size > HEADER_SIZE);

            let space_left = self.block_size - self.current_block_offset;

            // Fill up block; go to next block.
            if space_left < HEADER_SIZE {
                if space_left > 0 {
                    self.dst.write_all(&vec![0u8; space_left])?;
                }
                self.current_block_offset = 0;
            }

            let avail_for_data = self.block_size - self.current_block_offset - HEADER_SIZE;
            let data_frag_len = min(record.len(), avail_for_data);
            let first_frag = len == 0;

            let record_type = if first_frag && data_frag_len == record.len() {
                RecordType::Full
            } else if first_frag {
                RecordType::First
            } else if data_frag_len == record.len() {
                RecordType::Last
            } else {
                RecordType::Middle
            };

            len = self.emit_record(record_type, record, data_frag_len)?;
            record = &record[data_frag_len..];
        }

        Ok(len)
    }

    fn emit_record(&mut self, t: RecordType, data: &[u8], len: usize) -> KernelResult<usize> {
        let crc = crc32fast::hash(&data[0..len]);

        let mut header_bytes = crc.encode_fixed_vec();
        header_bytes.append(&mut (len as u32).encode_fixed_vec());
        header_bytes.append(&mut vec![t as u8]);

        let mut offset = 0;
        offset += self.dst.write(&header_bytes)?;
        offset += self.dst.write(&data[0..len])?;

        self.current_block_offset += offset;
        Ok(offset)
    }

    #[allow(dead_code)]
    pub(crate) fn flush(&mut self) -> KernelResult<()> {
        self.dst.flush()?;
        Ok(())
    }
}

pub(crate) struct LogReader<R: Read + Seek> {
    src: R,
    offset: usize,
    block_size: usize,
    head_scratch: [u8; HEADER_SIZE],
}

impl<R: Read + Seek> LogReader<R> {
    pub(crate) fn new(src: R) -> LogReader<R> {
        LogReader {
            src,
            offset: 0,
            block_size: BLOCK_SIZE,
            head_scratch: [0u8; HEADER_SIZE],
        }
    }

    /// EOF is signalled by Ok(0)
    pub(crate) fn read(&mut self, dst: &mut Vec<u8>) -> KernelResult<usize> {
        let mut dst_offset = 0;
        let mut head_pos = 0;

        dst.clear();

        loop {
            let leftover = self.block_size - self.offset;
            if leftover < HEADER_SIZE {
                // skip to next block
                if leftover != 0 {
                    let _ = self.src.seek(SeekFrom::Current((leftover) as i64))?;
                }
                self.offset = 0;
            }

            head_pos += self.src.read(&mut self.head_scratch[head_pos..])?;
            // EOF
            if head_pos == 0 {
                return Ok(dst_offset);
            } else if head_pos != HEADER_SIZE {
                continue;
            } else {
                head_pos = 0;
            }

            self.offset += HEADER_SIZE;

            let crc = u32::decode_fixed(&self.head_scratch[0..4]);
            let length = u32::decode_fixed(&self.head_scratch[4..8]) as usize;

            let mut buf = vec![0; length];

            self.src.read_exact(&mut buf)?;
            self.offset += length;
            dst_offset += length;

            if crc32fast::hash(&buf) != crc {
                return Err(KernelError::CrcMisMatch);
            }

            dst.append(&mut buf);

            if let RecordType::Full | RecordType::Last = RecordType::from(self.head_scratch[8]) {
                return Ok(dst_offset);
            }
        }
    }
}

