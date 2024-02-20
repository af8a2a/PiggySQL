use crate::errors::*;
use bytes::{Buf, BufMut, Bytes};
use crossbeam_skiplist::SkipMap;
use parking_lot::Mutex;
use std::fs::{File, OpenOptions};
use std::hash::Hasher;
use std::io::{BufWriter, Read, Write};
use std::path::Path;
use std::sync::Arc;
pub struct Wal {
    file: Arc<Mutex<BufWriter<File>>>,
}

impl Wal {
    pub fn create(path: impl AsRef<Path>) -> Result<Self> {
        Ok(Self {
            file: Arc::new(Mutex::new(BufWriter::new(
                OpenOptions::new()
                    .read(true)
                    .create_new(true)
                    .write(true)
                    .open(path)?,
            ))),
        })
    }
    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        let mut file = self.file.lock();
        let mut buf: Vec<u8> =
            Vec::with_capacity(key.len() + value.len() + std::mem::size_of::<u16>());
        let mut hasher = crc32fast::Hasher::new();
        //写入key-len key
        hasher.write_u16(key.len() as u16);
        buf.put_u16(key.len() as u16);
        hasher.write(&key);
        buf.put_slice(&key);
        //写入val-len val
        hasher.write_u16(value.len() as u16);
        buf.put_u16(value.len() as u16);
        hasher.write(&value);
        buf.put_slice(&value);
        //写入CRC32校验码
        buf.put_u32(hasher.finalize());
        file.write_all(&buf)?;
        Ok(())
    }
    pub fn sync(&self) -> Result<()> {
        let mut file = self.file.lock();
        file.flush()?;
        file.get_mut().sync_all()?;
        Ok(())
    }
    

}
