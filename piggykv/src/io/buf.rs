use std::fs::{File, OpenOptions};
use std::io;
use std::io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::PathBuf;
use std::sync::Arc;

use crate::KernelResult;

use super::{FileExtension, IoReader, IoType, IoWriter};

/// 使用BufReade和BufWriter实现的IOHandler
/// 目前是使用了Mutex实现其线程安全
/// 读方面可能有优化空间
#[derive(Debug)]
pub(crate) struct BufIoReader {
    gen: i64,
    dir_path: Arc<PathBuf>,
    reader: BufReaderWithPos<File>,
    extension: Arc<FileExtension>,
}

impl BufIoReader {
    pub(crate) fn new(
        dir_path: Arc<PathBuf>,
        gen: i64,
        extension: Arc<FileExtension>,
    ) -> KernelResult<Self> {
        let path = extension.path_with_gen(&dir_path, gen);

        let reader = BufReaderWithPos::new(
            OpenOptions::new()
                .create(true)
                .write(true)
                .read(true)
                .open(path)?,
        )?;

        Ok(BufIoReader {
            gen,
            dir_path,
            reader,
            extension,
        })
    }
}

#[derive(Debug)]
pub(crate) struct BufIoWriter {
    writer: BufWriterWithPos<File>,
}

impl BufIoWriter {
    pub(crate) fn new(
        dir_path: Arc<PathBuf>,
        gen: i64,
        extension: Arc<FileExtension>,
    ) -> KernelResult<Self> {
        // 通过路径构造写入器
        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .read(true)
            .open(extension.path_with_gen(&dir_path, gen))?;

        Ok(BufIoWriter {
            writer: BufWriterWithPos::new(file)?,
        })
    }
}

impl Read for BufIoReader {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.reader.read(buf)
    }
}

impl Seek for BufIoReader {
    fn seek(&mut self, seek: SeekFrom) -> io::Result<u64> {
        self.reader.seek(seek)
    }
}

impl IoReader for BufIoReader {
    fn get_gen(&self) -> i64 {
        self.gen
    }

    fn get_path(&self) -> PathBuf {
        self.extension.path_with_gen(&self.dir_path, self.gen)
    }

    fn get_type(&self) -> IoType {
        IoType::Buf
    }
}

impl Write for BufIoWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.writer.write(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.writer.flush()
    }
}

impl Seek for BufIoWriter {
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        self.writer.seek(pos)
    }
}

impl IoWriter for BufIoWriter {
    fn current_pos(&mut self) -> KernelResult<u64> {
        Ok(self.writer.pos)
    }
}

#[derive(Debug)]
pub(crate) struct BufReaderWithPos<R: Read + Seek> {
    reader: BufReader<R>,
    pos: u64,
}

impl<R: Read + Seek> BufReaderWithPos<R> {
    fn new(mut inner: R) -> KernelResult<Self> {
        let pos = inner.stream_position()?;
        Ok(BufReaderWithPos {
            reader: BufReader::new(inner),
            pos,
        })
    }
}

impl<R: Read + Seek> Read for BufReaderWithPos<R> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let len = self.reader.read(buf)?;
        self.pos += len as u64;
        Ok(len)
    }
}

impl<R: Read + Seek> Seek for BufReaderWithPos<R> {
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        self.pos = self.reader.seek(pos)?;
        Ok(self.pos)
    }
}

#[derive(Debug)]
pub(crate) struct BufWriterWithPos<W: Write + Seek> {
    writer: BufWriter<W>,
    pos: u64,
}

impl<W: Write + Seek> BufWriterWithPos<W> {
    fn new(mut inner: W) -> KernelResult<Self> {
        let pos = inner.stream_position()?;
        Ok(BufWriterWithPos {
            writer: BufWriter::new(inner),
            pos,
        })
    }
}

impl<W: Write + Seek> Write for BufWriterWithPos<W> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let len = self.writer.write(buf)?;
        self.pos += len as u64;
        Ok(len)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.writer.flush()
    }
}

impl<W: Write + Seek> Seek for BufWriterWithPos<W> {
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        self.pos = self.writer.seek(pos)?;
        Ok(self.pos)
    }
}
