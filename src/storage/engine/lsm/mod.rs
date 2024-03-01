use crossbeam_skiplist::SkipMap;

use parking_lot::Mutex;
use tracing::debug;

use crate::errors::Result;

use std::hash::Hasher;
use std::io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write};

use std::ops::{Bound, DerefMut};
use std::path::PathBuf;
use std::sync::Arc;

use super::{KvScan, StorageEngine};

/// A very simple variant of BitCask, itself a very simple log-structured
/// key-value engine used e.g. by the Riak database. It is not compatible with
/// BitCask databases generated by other implementations. See:
/// https://riak.com/assets/bitcask-intro.pdf
///
/// BitCask writes key-value pairs to an append-only log file, and keeps a
/// mapping of keys to file positions in memory. All live keys must fit in
/// memory. Deletes write a tombstone value to the log file. To remove old
/// garbage, logs can be compacted by writing new logs containing only live
/// data, skipping replaced values and tombstones.
///
/// This implementation makes several significant simplifications over
/// standard BitCask:
///
/// - Instead of writing multiple fixed-size log files, it uses a single
///   append-only log file of arbitrary size. This increases the compaction
///   volume, since the entire log file must be rewritten on every compaction,
///   and can exceed the filesystem's file size limit, but ToyDB databases are
///   expected to be small.
///
/// - Compactions lock the database for reads and writes. This is ok since ToyDB
///   only compacts during node startup and files are expected to be small.
///
/// - Hint files are not used, the log itself is scanned when opened to
///   build the keydir. Hint files only omit values, and ToyDB values are
///   expected to be small, so the hint files would be nearly as large as
///   the compacted log files themselves.
///
/// - Log entries don't contain timestamps or checksums.
///
/// The structure of a log entry is:
///
/// - Key length as big-endian u32.
/// - Value length as big-endian i32, or -1 for tombstones.
/// - Key as raw bytes (max 2 GB).
/// - Value as raw bytes (max 2 GB).
pub struct BitCask {
    /// The active append-only log file.
    log: Log,
    /// Maps keys to a value position and length in the log file.
    keydir: KeyDir,
}

/// Maps keys to a value position and length in the log file.
type KeyDir = SkipMap<Vec<u8>, (u64, u32)>;

impl BitCask {
    /// Opens or creates a BitCask database in the given file.
    pub fn new(path: PathBuf) -> Result<Self> {
        let log = Log::new(path)?;
        let keydir = log.build_keydir()?;
        Ok(Self { log, keydir })
    }

    // /// Opens a BitCask database, and automatically compacts it if the amount
    // /// of garbage exceeds the given ratio when opened.
    pub fn new_compact(path: PathBuf, _garbage_ratio_threshold: f64) -> Result<Self> {
        let mut s = Self::new(path)?;

        // let status = s.status()?;
        // let garbage_ratio = status.garbage_disk_size as f64 / status.total_disk_size as f64;
        // if status.garbage_disk_size > 0 && garbage_ratio >= garbage_ratio_threshold {
        //     log::info!(
        //         "Compacting {} to remove {:.3}MB garbage ({:.0}% of {:.3}MB)",
        //         s.log.path.display(),
        //         status.garbage_disk_size / 1024 / 1024,
        //         garbage_ratio * 100.0,
        //         status.total_disk_size / 1024 / 1024
        //     );
        s.compact()?;
        //     log::info!(
        //         "Compacted {} to size {:.3}MB",
        //         s.log.path.display(),
        //         (status.total_disk_size - status.garbage_disk_size) / 1024 / 1024
        //     );
        // }

        Ok(s)
    }
}

impl std::fmt::Display for BitCask {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "bitcask")
    }
}

impl StorageEngine for BitCask {
    fn delete(&self, key: &[u8]) -> Result<()> {
        self.log.write_entry(key, None)?;
        self.keydir.remove(key);
        Ok(())
    }

    fn flush(&self) -> Result<()> {
        Ok(self.log.file.lock().sync_all()?)
    }

    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        if let Some((value_pos, value_len)) =
            self.keydir.get(key).map(|entry| entry.value().clone())
        {
            Ok(Some(self.log.read_value(value_pos, value_len)?))
        } else {
            Ok(None)
        }
    }

    fn scan(&self, range: impl std::ops::RangeBounds<Vec<u8>>) -> Result<KvScan> {
        Ok(Box::new(
            ScanIterator {
                inner: self
                    .keydir
                    .range((range.start_bound().cloned(), range.end_bound().cloned())),
                log: &self.log,
            }
            .collect::<Vec<_>>()
            .into_iter(),
        ))
    }

    fn set(&self, key: &[u8], value: Vec<u8>) -> Result<()> {
        let (pos, len) = self.log.write_entry(key, Some(&*value))?;
        let value_len = value.len() as u32;
        self.keydir.insert(
            key.to_vec(),
            (pos + len as u64 - value_len as u64, value_len),
        );
        Ok(())
    }

    // fn status(&mut self) -> Result<Status> {
    //     let keys = self.keydir.len() as u64;
    //     let size = self
    //         .keydir
    //         .iter()
    //         .fold(0, |size, (key, (_, value_len))| size + key.len() as u64 + *value_len as u64);
    //     let total_disk_size = self.log.file.metadata()?.len();
    //     let live_disk_size = size + 8 * keys; // account for length prefixes
    //     let garbage_disk_size = total_disk_size - live_disk_size;
    //     Ok(Status {
    //         name: self.to_string(),
    //         keys,
    //         size,
    //         total_disk_size,
    //         live_disk_size,
    //         garbage_disk_size,
    //     })
    // }
}

pub struct ScanIterator<'a> {
    inner: crossbeam_skiplist::map::Range<
        'a,
        Vec<u8>,
        (Bound<Vec<u8>>, Bound<Vec<u8>>),
        Vec<u8>,
        (u64, u32),
    >,
    log: &'a Log,
}

impl<'a> ScanIterator<'a> {
    fn map(&mut self, item: (&Vec<u8>, &(u64, u32))) -> <Self as Iterator>::Item {
        let (key, (value_pos, value_len)) = item;
        Ok((key.clone(), self.log.read_value(*value_pos, *value_len)?))
    }
}

impl<'a> Iterator for ScanIterator<'a> {
    type Item = Result<(Vec<u8>, Vec<u8>)>;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner
            .next()
            .map(|item| self.map((item.key(), item.value())))
    }
}

impl<'a> DoubleEndedIterator for ScanIterator<'a> {
    fn next_back(&mut self) -> Option<Self::Item> {
        self.inner
            .next_back()
            .map(|item| self.map((item.key(), item.value())))
    }
}

impl BitCask {
    /// Compacts the current log file by writing out a new log file containing
    /// only live keys and replacing the current file with it.
    pub fn compact(&mut self) -> Result<()> {
        let mut tmp_path = self.log.path.clone();
        tmp_path.set_extension("new");
        let (mut new_log, new_keydir) = self.write_log(tmp_path)?;

        std::fs::rename(&new_log.path, &self.log.path)?;
        new_log.path = self.log.path.clone();
        self.log = new_log;
        self.keydir = new_keydir;
        Ok(())
    }

    /// Writes out a new log file with the live entries of the current log file
    /// and returns it along with its keydir. Entries are written in key order.
    fn write_log(&self, path: PathBuf) -> Result<(Log, KeyDir)> {
        let new_keydir = KeyDir::new();
        let new_log = Log::new(path)?;
        new_log.file.lock().set_len(0)?; // truncate file if it exists
        for (key, (value_pos, value_len)) in self
            .keydir
            .iter()
            .map(|entry| (entry.key().clone(), entry.value().clone()))
        {
            let value = self.log.read_value(value_pos, value_len)?;
            let (pos, len) = new_log.write_entry(&key, Some(&value))?;
            new_keydir.insert(
                key.clone(),
                (pos + len as u64 - value_len as u64, value_len),
            );
        }
        Ok((new_log, new_keydir))
    }
}

/// Attempt to flush the file when the database is closed.
impl Drop for BitCask {
    fn drop(&mut self) {
        if let Err(_error) = self.flush() {}
    }
}

/// A BitCask append-only log file, containing a sequence of key/value
/// entries encoded as follows;
///
/// - Key length as big-endian u32.
/// - Value length as big-endian i32, or -1 for tombstones.
/// - Key as raw bytes (max 2 GB).
/// - Value as raw bytes (max 2 GB).
struct Log {
    /// Path to the log file.
    path: PathBuf,
    /// The opened file containing the log.
    file: Arc<Mutex<std::fs::File>>,
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
        let mut len_buf = [0u8; 4];
        let mut checksum_buf = [0u8; 4];
        let keydir = KeyDir::new();
        let file_len = file.metadata()?.len();
        let mut r = BufReader::new(file.deref_mut());
        let mut pos = r.seek(SeekFrom::Start(0))?;

        while pos < file_len {
            // Read the next entry from the file, returning the key, value
            // position, and value length or None for tombstones.
            let result = || -> std::result::Result<(Vec<u8>, u64, Option<u32>), std::io::Error> {
                let mut hasher = crc32fast::Hasher::new();
                r.read_exact(&mut checksum_buf)?;
                let checksum = u32::from_be_bytes(checksum_buf);

                r.read_exact(&mut len_buf)?;
                let key_len = u32::from_be_bytes(len_buf);
                r.read_exact(&mut len_buf)?;
                let value_len = i32::from_be_bytes(len_buf);
                let value_len_or_tombstone = match value_len {
                    l if l >= 0 => Some(l as u32),
                    _ => None, // -1 for tombstones
                };

                let value_pos = pos + 4 + 4 + key_len as u64;

                let mut key = vec![0; key_len as usize];
                r.read_exact(&mut key)?;
                hasher.write_u32(key_len);
                hasher.write_i32(value_len);
                hasher.write(&key);

                let expect_checksum = hasher.finalize();
                if expect_checksum != checksum {
                    debug!("checksum mismatch: {} != {}", expect_checksum, checksum);
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::UnexpectedEof,
                        "checksum mismatchh",
                    ));
                }
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

        Ok(keydir)
    }

    /// Reads a value from the log file.
    fn read_value(&self, value_pos: u64, value_len: u32) -> Result<Vec<u8>> {
        let mut file = self.file.lock();

        let mut value = vec![0; value_len as usize];
        file.seek(SeekFrom::Start(value_pos))?;
        file.read_exact(&mut value)?;
        Ok(value)
    }

    /// Appends a key/value entry to the log file, using a None value for
    /// tombstones. It returns the position and length of the entry.
    fn write_entry(&self, key: &[u8], value: Option<&[u8]>) -> Result<(u64, u32)> {
        let mut file = self.file.lock();

        let key_len = key.len() as u32;
        let value_len = value.map_or(0, |v| v.len() as u32);
        let value_len_or_tombstone = value.map_or(-1, |v| v.len() as i32);
        let len = 4 + 4 + key_len + value_len + 4;
        let mut hasher = crc32fast::Hasher::new();
        hasher.write_u32(key_len);
        hasher.write_i32(value_len_or_tombstone);
        hasher.write(key);

        let pos = file.seek(SeekFrom::End(0))?;
        let mut w = BufWriter::with_capacity(len as usize, file.deref_mut());
        w.write_all(&hasher.finalize().to_be_bytes())?;
        w.write_all(&key_len.to_be_bytes())?;
        w.write_all(&value_len_or_tombstone.to_be_bytes())?;
        w.write_all(key)?;
        if let Some(value) = value {
            w.write_all(value)?;
        }

        w.flush()?;

        Ok((pos, len))
    }
    #[allow(dead_code)]
    #[cfg(test)]
    /// Prints the entire log file to the given writer in human-readable form.
    fn print<W: Write>(&mut self, w: &mut W) -> Result<()> {
        let mut file = self.file.lock();
        let mut len_buf = [0u8; 4];
        let file_len = file.metadata()?.len();
        let mut r = BufReader::new(file.deref_mut());
        let mut pos = r.seek(SeekFrom::Start(0))?;
        let mut idx = 0;

        while pos < file_len {
            writeln!(w, "entry = {}, offset {}", idx, pos)?;

            r.read_exact(&mut len_buf)?;
            let key_len = u32::from_be_bytes(len_buf);
            writeln!(w, "klen  = {} {:x?}", key_len, len_buf)?;

            r.read_exact(&mut len_buf)?;
            let value_len_or_tombstone = i32::from_be_bytes(len_buf); // NB: -1 for tombstones
            let value_len = value_len_or_tombstone.max(0) as u32;
            writeln!(w, "vlen  = {} {:x?}", value_len_or_tombstone, len_buf)?;

            let mut key = vec![0; key_len as usize];
            r.read_exact(&mut key)?;
            write!(w, "key   = ")?;
            if let Ok(str) = std::str::from_utf8(&key) {
                write!(w, r#""{}" "#, str)?;
            }
            writeln!(w, "{:x?}", key)?;

            let mut value = vec![0; value_len as usize];
            r.read_exact(&mut value)?;
            write!(w, "value = ")?;
            if value_len_or_tombstone < 0 {
                write!(w, "tombstone ")?;
            } else if let Ok(str) = std::str::from_utf8(&value) {
                if str.chars().all(|c| !c.is_control()) {
                    write!(w, r#""{}" "#, str)?;
                }
            }
            write!(w, "{:x?}\n\n", value)?;

            pos += 4 + 4 + key_len as u64 + value_len as u64;
            idx += 1;
        }
        Ok(())
    }
}
#[cfg(test)]
mod test {
    use super::*;
    use crate::storage::engine::tests::test_engine;
    test_engine!({
        let path = tempdir::TempDir::new("piggydb")
            .unwrap()
            .path()
            .join("piggydb");
        BitCask::new(path)?
    });
}
