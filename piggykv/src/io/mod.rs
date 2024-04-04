pub(crate) mod buf;
pub(crate) mod direct;

use std::{
    fs, io::{Read, Seek, Write}, path::{Path, PathBuf}, sync::Arc
};

use crate::KernelResult;

use self::{
    buf::{BufIoReader, BufIoWriter},
    direct::{DirectIoReader, DirectIoWriter},
};

#[derive(Debug, Copy, Clone)]
pub enum FileExtension {
    Log,
    SSTable,
    Manifest,
}

impl FileExtension {
    pub(crate) fn extension_str(&self) -> &'static str {
        match self {
            FileExtension::Log => "log",
            FileExtension::SSTable => "sst",
            FileExtension::Manifest => "manifest",
        }
    }

    /// 对文件夹路径填充日志文件名
    pub(crate) fn path_with_gen(&self, dir: &Path, gen: i64) -> PathBuf {
        dir.join(format!("{gen}.{}", self.extension_str()))
    }
}

pub struct IoFactory {
    dir_path: Arc<PathBuf>,
    extension: Arc<FileExtension>,
}

#[derive(PartialEq, Copy, Clone, Debug)]
pub enum IoType {
    Buf,
    Direct,
}

impl IoFactory {
    #[inline]
    pub fn reader(&self, gen: i64, io_type: IoType) -> KernelResult<Box<dyn IoReader>> {
        let dir_path = Arc::clone(&self.dir_path);
        let extension = Arc::clone(&self.extension);

        Ok(match io_type {
            IoType::Buf => Box::new(BufIoReader::new(dir_path, gen, extension)?),
            IoType::Direct => Box::new(DirectIoReader::new(dir_path, gen, extension)?),
        })
    }

    #[inline]
    pub fn writer(&self, gen: i64, io_type: IoType) -> KernelResult<Box<dyn IoWriter>> {
        let dir_path = Arc::clone(&self.dir_path);
        let extension = Arc::clone(&self.extension);

        Ok(match io_type {
            IoType::Buf => Box::new(BufIoWriter::new(dir_path, gen, extension)?),
            IoType::Direct => Box::new(DirectIoWriter::new(dir_path, gen, extension)?),
        })
    }

    #[inline]
    pub fn get_path(&self) -> &PathBuf {
        &self.dir_path
    }

    #[inline]
    pub fn new(dir_path: impl Into<PathBuf>, extension: FileExtension) -> KernelResult<Self> {
        let path_buf = dir_path.into();
        // 创建文件夹（如果他们缺失）
        fs::create_dir_all(&path_buf)?;
        let dir_path = Arc::new(path_buf);
        let extension = Arc::new(extension);

        Ok(Self {
            dir_path,
            extension,
        })
    }

    #[inline]
    pub fn clean(&self, gen: i64) -> KernelResult<()> {
        fs::remove_file(self.extension.path_with_gen(&self.dir_path, gen))?;
        Ok(())
    }

    #[inline]
    pub fn exists(&self, gen: i64) -> KernelResult<bool> {
        let path = self.extension.path_with_gen(&self.dir_path, gen);

        Ok(path.exists())
    }
}

pub trait IoReader: Send + Sync + 'static + Read + Seek {
    fn get_gen(&self) -> i64;

    fn get_path(&self) -> PathBuf;

    #[inline]
    fn file_size(&self) -> KernelResult<u64> {
        let path_buf = self.get_path();
        Ok(fs::metadata(path_buf)?.len())
    }

    fn get_type(&self) -> IoType;
}

pub trait IoWriter: Send + Sync + 'static + Write + Seek {
    fn current_pos(&mut self) -> KernelResult<u64>;
}
