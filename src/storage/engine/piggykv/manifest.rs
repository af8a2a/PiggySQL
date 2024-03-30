use std::fs::{File, OpenOptions};
use std::io::{Read, Write};
use std::path::Path;
use std::sync::Arc;

use bytes::{Buf, BufMut};
use parking_lot::{Mutex, MutexGuard};
use serde::{Deserialize, Serialize};
use tracing::{error};
use crate::errors::Result;

use super::compact::CompactionTask;


pub struct Manifest {
    file: Arc<Mutex<File>>,
}

#[derive(Serialize, Deserialize)]
pub enum ManifestRecord {
    Flush(usize),
    NewMemtable(usize),
    Compaction(CompactionTask, Vec<usize>),
}

impl Manifest {
    pub fn create(path: impl AsRef<Path>) -> Result<Self> {
        Ok(Self {
            file: Arc::new(Mutex::new(
                OpenOptions::new()
                    .read(true)
                    .create_new(true)
                    .write(true)
                    .open(path)
                    .expect("failed to create manifest"),
            )),
        })
    }
    pub fn sync(&self) -> Result<()> {
        self.file.lock().sync_all()?;
        Ok(())
    }
    pub fn recover(path: impl AsRef<Path>) -> Result<(Self, Vec<ManifestRecord>)> {
        let mut file = OpenOptions::new()
            .read(true)
            .append(true)
            .open(path)
            .expect("failed to recover manifest");
        let mut buf = Vec::new();
        file.read_to_end(&mut buf)?;
        let mut buf_ptr = buf.as_slice();
        let mut records = Vec::new();
        while buf_ptr.has_remaining() {
            let len = buf_ptr.get_u64();
            let slice = &buf_ptr[..len as usize];
            let json = serde_json::from_slice::<ManifestRecord>(slice).unwrap();
            buf_ptr.advance(len as usize);
            let checksum = buf_ptr.get_u32();
            if checksum != crc32fast::hash(slice) {
                error!("checksum mismatched!");
                panic!("checksum mismatched!");
            }
            records.push(json);
        }
        Ok((
            Self {
                file: Arc::new(Mutex::new(file)),
            },
            records,
        ))
    }

    pub fn add_record(
        &self,
        _state_lock_observer: &MutexGuard<()>,
        record: ManifestRecord,
    ) -> Result<()> {
        self.add_record_when_init(record)
    }

    pub fn add_record_when_init(&self, record: ManifestRecord) -> Result<()> {
        let mut file = self.file.lock();
        let mut buf = serde_json::to_vec(&record).unwrap();
        let hash = crc32fast::hash(&buf);
        file.write_all(&(buf.len() as u64).to_be_bytes())?;
        buf.put_u32(hash);
        file.write_all(&buf)?;
        file.sync_all()?;
        Ok(())
    }
}
