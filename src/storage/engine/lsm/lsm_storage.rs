use std::fs::File;
use std::io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::ops::DerefMut;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::vec::IntoIter;

use crossbeam_skiplist::SkipMap;
use parking_lot::{Mutex, RwLock};

use super::iterators::{MergeIter, StorageIter};
use super::lsm_iterator::LsmIter;
use super::memtable::MemTable;
use crate::errors::Result;
use crate::storage::engine::{KvScan, StorageEngine};
type KeyDir = Arc<SkipMap<Vec<u8>, (u64, i64)>>;
struct Log {
    /// Path to the log file.
    path: PathBuf,
    /// The opened file containing the log.
    file: Arc<Mutex<File>>,
}
impl Log {
    /// Opens a log file, or creates one if it does not exist. Takes out an
    /// exclusive lock on the file until it is closed, or errors if the lock is
    /// already held.
    fn new(path: PathBuf) -> Result<Self> {
        if let Some(dir) = path.parent() {
            std::fs::create_dir_all(dir)?
        }
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&path)?;
        Ok(Self {
            path,
            file: Arc::new(Mutex::new(file)),
        })
    }

    /// Builds a keydir by scanning the log file. If an incomplete entry is
    /// encountered, it is assumed to be caused by an incomplete write operation
    /// and the remainder of the file is truncated.
    fn build_keydir(&self) -> Result<KeyDir> {
        let mut file = self.file.lock();
        let mut len_buf = [0u8; 8];
        let keydir = SkipMap::new();
        let file_len = file.metadata()?.len();
        let mut r = BufReader::new(file.deref_mut());
        let mut pos = r.seek(SeekFrom::Start(0))?;

        while pos < file_len {
            // Read the next entry from the file, returning the key, value
            // position, and value length or None for tombstones.
            let result = || -> std::result::Result<(Vec<u8>, u64, Option<i64>), std::io::Error> {
                r.read_exact(&mut len_buf)?;
                let key_len = u64::from_be_bytes(len_buf);
                r.read_exact(&mut len_buf)?;
                let value_len_or_tombstone = match i64::from_be_bytes(len_buf) {
                    l if l >= 0 => Some(l as i64),
                    _ => None, // -1 for tombstones
                };
                let value_pos = pos + 4 + 4 + key_len as u64;

                let mut key = vec![0; key_len as usize];
                r.read_exact(&mut key)?;

                if let Some(value_len) = value_len_or_tombstone {
                    if value_pos + value_len as u64 > file_len {
                        return Err(std::io::Error::new(
                            std::io::ErrorKind::UnexpectedEof,
                            "value extends beyond end of file",
                        ));
                    }
                    r.seek_relative(value_len as i64)?; // avoids discarding buffer
                }

                Ok((key, value_pos, value_len_or_tombstone))
            }();

            match result {
                // Populate the keydir with the entry, or remove it on tombstones.
                Ok((key, value_pos, Some(value_len))) => {
                    keydir.insert(key, (value_pos, value_len));
                    pos = value_pos + value_len as u64;
                }
                Ok((key, value_pos, None)) => {
                    keydir.remove(&key);
                    pos = value_pos;
                }
                // If an incomplete entry was found at the end of the file, assume an
                // incomplete write and truncate the file.
                Err(err) if err.kind() == std::io::ErrorKind::UnexpectedEof => {
                    file.set_len(pos)?;
                    break;
                }
                Err(err) => return Err(err.into()),
            }
        }

        Ok(Arc::new(keydir))
    }

    /// Reads a value from the log file.
    fn read_value(&self, value_pos: u64, value_len: i64) -> Result<Vec<u8>> {
        let mut value = vec![0; value_len as usize];
        let mut file = self.file.lock();
        file.seek(SeekFrom::Start(value_pos))?;
        file.read_exact(&mut value)?;
        Ok(value)
    }

    /// Appends a key/value entry to the log file, using a None value for
    /// tombstones. It returns the position and length of the entry.
    fn write_entry(&self, key: &[u8], value: Option<&[u8]>) -> Result<(u64, u64)> {
        let mut file = self.file.lock();

        let key_len = key.len() as u64;
        let value_len = value.map_or(0, |v| v.len() as u64);
        let value_len_or_tombstone = value.map_or(-1, |v| v.len() as i32);
        let len = 4 + 4 + key_len + value_len;

        let pos = file.seek(SeekFrom::End(0))?;
        let mut w = BufWriter::with_capacity(len as usize, file.deref_mut());
        w.write_all(&key_len.to_be_bytes())?;
        w.write_all(&value_len_or_tombstone.to_be_bytes())?;
        w.write_all(key)?;
        if let Some(value) = value {
            w.write_all(value)?;
        }
        w.flush()?;

        Ok((pos, len))
    }
}

#[derive(Clone)]
pub struct LsmStorageInner {
    /// The current memtable.
    memtable: Arc<MemTable>,
    /// Immutable memTables, from earliest to latest.
    imm_memtables: Vec<Arc<MemTable>>,
    file: Arc<Log>,
    keydir: KeyDir,
}

impl LsmStorageInner {
    fn create(path: impl AsRef<Path>) -> Self {
        Self {
            memtable: Arc::new(MemTable::create()),
            imm_memtables: vec![],
            file: Arc::new(Log {
                path: path.as_ref().to_path_buf(),
                file: Arc::new(Mutex::new(File::open(path).unwrap())),
            }),
            keydir: Arc::new(SkipMap::new()),
        }
    }
}

/// The storage interface of the LSM tree.
pub struct LsmStorage {
    inner: Arc<RwLock<Arc<LsmStorageInner>>>,
    flush_lock: Mutex<()>,
}

impl LsmStorage {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        Ok(Self {
            inner: Arc::new(RwLock::new(Arc::new(LsmStorageInner::create(path)))),
            flush_lock: Mutex::new(()),
        })
    }
}

impl std::fmt::Display for LsmStorage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "LsmStorage")
    }
}

impl StorageEngine for LsmStorage {
    fn set(&self, key: &[u8], value: Vec<u8>) -> Result<()> {
        assert!(!key.is_empty(), "key cannot be empty");
        assert!(!value.is_empty(), "value cannot be empty");

        let session = self.inner.read();
        session.memtable.set(key, value);

        Ok(())
    }

    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let snapshot = {
            let session = self.inner.read();
            Arc::clone(&session)
        };

        // Search in the current memtable.
        if let Some(value) = snapshot.memtable.get(key) {
            match value.is_empty() {
                true => return Ok(None),
                false => return Ok(Some(value)),
            }
        }

        // Search in immutable memtables.
        for memtable in snapshot.imm_memtables.iter().rev() {
            if let Some(value) = memtable.get(key) {
                match value.is_empty() {
                    true => return Ok(None),
                    false => return Ok(Some(value)),
                }
            }
        }

        // Search in disk.
        let offset = snapshot.keydir.get(key);
        match offset {
            Some(entry) => {
                let offset = entry.value();
                let val = snapshot.file.read_value(offset.0, offset.1)?;
                Ok(Some(val))
            }
            None => Ok(None),
        }
    }

    fn delete(&self, key: &[u8]) -> Result<()> {
        assert!(!key.is_empty(), "key cannot be empty");

        let session = self.inner.read();
        session.memtable.set(key, vec![]);
        session.keydir.remove(key);
        Ok(())
    }

    fn scan(&self, range: impl std::ops::RangeBounds<Vec<u8>>) -> Result<KvScan> {
        let snapshot = {
            let session = self.inner.read();
            Arc::clone(&session)
        };

        let mut memtable_iters = vec![];
        memtable_iters.reserve(snapshot.imm_memtables.len() + 1);
        memtable_iters.push(Box::new(
            snapshot
                .memtable
                .scan((range.start_bound(), range.end_bound())),
        ));
        for memtable in snapshot.imm_memtables.iter().rev() {
            memtable_iters.push(Box::new(
                memtable.scan((range.start_bound(), range.end_bound())),
            ));
        }
        let memtable_merge_iter = MergeIter::create(memtable_iters)?;
        let disk_iter = snapshot
            .keydir
            .range((range.start_bound().cloned(), range.end_bound().cloned()))
            .map(|entry| (entry.key().clone(), entry.value().clone()))
            .collect::<Vec<_>>()
            .into_iter();
        let disk_iter = DiskIter {
            offset_iter: disk_iter,
            file: Arc::clone(&snapshot.file),
        };
        let memtable_merge_iter = MergeIter::create(memtable_merge_iter, disk_iter);
        Ok(Box::new(LsmIter::create(memtable_merge_iter)))
    }

    fn flush(&self) -> Result<()> {
        // let _flush_guard = self.flush_lock.lock();

        // let memtable_to_flush;
        // let sstable_id;

        // // Move mutable memtable to immutable memtables.
        // {
        //     let mut session = self.inner.write();

        //     // Swap the current memtable with a new one.
        //     let mut snapshot = session.as_ref().clone();
        //     let memtable = std::mem::replace(&mut snapshot.memtable, Arc::new(MemTable::create()));
        //     memtable_to_flush = memtable.clone();
        //     sstable_id = snapshot.next_sst_id;

        //     // Add the memtable to the immutable memtables.
        //     snapshot.imm_memtables.push(memtable);

        //     // Update the snapshot.
        //     *session = Arc::new(snapshot);
        // }

        // // At this point, the old memtable should be disabled for write, and all write threads
        // // should be operating on the new memtable. We can safely flush the old memtable to
        // // disk.

        // let mut sstable_builder = SsTableBuilder::new(4096);
        // memtable_to_flush.flush(&mut sstable_builder)?;
        // let sstable = Arc::new(sstable_builder.build(
        //     sstable_id,
        //     Some(self.block_cache.clone()),
        //     self.path.join(format!("{:05}.sst", sstable_id)),
        // )?);

        // // Add the flushed L0 table to the list.
        // {
        //     let mut session = self.inner.write();
        //     let mut snapshot = session.as_ref().clone();
        //     // Remove the memtable from the immutable memtables.
        //     snapshot.imm_memtables.pop();
        //     // Add L0 table
        //     snapshot.l0_sstables.push(sstable);
        //     // Update SST ID
        //     snapshot.next_sst_id += 1;
        //     // Update the snapshot.
        //     *session = Arc::new(snapshot);
        // }

        Ok(())
    }
}

pub struct DiskIter {
    offset_iter: IntoIter<(Vec<u8>, (u64, i64))>,
    front_key: Option<Vec<u8>>,
    back_index: Option<Vec<u8>>,
    file: Arc<Log>,
}
impl Iterator for DiskIter {
    type Item = Result<(Vec<u8>, Vec<u8>)>;

    fn next(&mut self) -> Option<Self::Item> {
        let (key, (value_pos, value_len)) = self.offset_iter.next()?;
        let item = self.file.read_value(value_pos, value_len).unwrap();
        Some(Ok((key, item)))
    }
}
impl DoubleEndedIterator for DiskIter {
    fn next_back(&mut self) -> Option<Self::Item> {
        let (key, (value_pos, value_len)) = self.offset_iter.next_back()?;
        let item = self.file.read_value(value_pos, value_len).unwrap();
        Some(Ok((key, item)))
    }
}
impl StorageIter for DiskIter {
    fn front_entry(&self) -> Option<(Vec<u8>, Vec<u8>)> {
        todo!()
    }

    fn back_entry(&self) -> Option<(Vec<u8>, Vec<u8>)> {
        todo!()
    }

    fn is_valid(&self) -> bool {
        todo!()
    }

    fn try_next(&mut self) -> Result<Option<(Vec<u8>, Vec<u8>)>> {
        todo!()
    }

    fn try_next_back(&mut self) -> Result<Option<(Vec<u8>, Vec<u8>)>> {
        todo!()
    }
}
