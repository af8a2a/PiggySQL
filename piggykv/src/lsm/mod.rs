use std::{
    ffi::OsStr, fs, path::{Path, PathBuf}, sync::Arc, time::Duration
};

use bytes::Bytes;
use fslock::LockFile;
use tokio::{sync::{mpsc::{channel, error::TrySendError, Sender}, oneshot}, time};
use tracing::error;

use crate::{error::KernelError, io::FileExtension, KernelResult};
use async_trait::async_trait;
use self::{compactor::{CompactTask, Compactor, MergeShardingVec}, mem_table::{key_value_bytes_len, KeyValue, MemTable}, mvcc::{CheckType, Transaction}, sstable::scope::Scope, storage::{Config, Gen, StoreInner}, version::Version};

pub mod iterator;
pub mod mem_table;
pub mod sstable;
pub mod storage;
pub mod wal;
pub mod trigger;
pub mod compactor;
pub mod version;
pub mod mvcc;

pub(crate) const DEFAULT_LOCK_FILE: &str = "Piggy.lock";

const MAX_LEVEL: usize = 4;

/// KeyValue数据分片，尽可能将数据按给定的分片大小：file_size，填满一片（可能会溢出一些）
/// 保持原有数据的顺序进行分片，所有第一片分片中最后的值肯定会比其他分片开始的值Key排序较前（如果vec_data是以Key从小到大排序的话）
fn data_sharding(mut vec_data: Vec<KeyValue>, file_size: usize) -> MergeShardingVec {
    // 向上取整计算SSTable数量
    let part_size =
        (vec_data.iter().map(key_value_bytes_len).sum::<usize>() + file_size - 1) / file_size;

    vec_data.reverse();
    let mut vec_sharding = vec![(0, Vec::new()); part_size];
    let slice = vec_sharding.as_mut_slice();

    for i in 0..part_size {
        // 减小create_gen影响的时间
        slice[i].0 = Gen::create();
        let mut data_len = 0;
        while !vec_data.is_empty() {
            if let Some(key_value) = vec_data.pop() {
                data_len += key_value_bytes_len(&key_value);
                if data_len >= file_size && i < part_size - 1 {
                    slice[i + 1].1.push(key_value);
                    break;
                }
                slice[i].1.push(key_value);
            } else {
                break;
            }
        }
    }
    // 过滤掉没有数据的切片
    vec_sharding.retain(|(_, vec)| !vec.is_empty());
    vec_sharding
}

/// 使用Version进行Key查询，当触发Seek Miss的阈值时，
/// 使用其第一次Miss的Level进行Seek Compaction
fn query_and_compaction(
    key: &[u8],
    version: &Version,
    compactor_tx: &Sender<CompactTask>,
) -> KernelResult<Option<KeyValue>> {
    let (value_option, miss_option) = version.query(key)?;

    if let Some(miss_scope) = miss_option {
        if let Err(TrySendError::Closed(_)) = compactor_tx.try_send(CompactTask::Seek(miss_scope)) {
            return Err(KernelError::ChannelClose);
        }
    }

    if let Some(key_value) = value_option {
        return Ok(Some(key_value));
    }
    Ok(None)
}


/// 现有日志文件序号排序
fn sorted_gen_list(file_path: &Path, extension: FileExtension) -> KernelResult<Vec<i64>> {
    let mut gen_list: Vec<i64> = fs::read_dir(file_path)?
        .flat_map(|res| -> KernelResult<_> { Ok(res?.path()) })
        .filter(|path| {
            path.is_file() && path.extension() == Some(extension.extension_str().as_ref())
        })
        .flat_map(|path| {
            path.file_name()
                .and_then(OsStr::to_str)
                .map(|s| s.trim_end_matches(format!(".{}", extension.extension_str()).as_str()))
                .map(str::parse::<i64>)
        })
        .flatten()
        .collect();
    // 对序号进行排序
    gen_list.sort_unstable();
    // 返回排序好的Vec
    Ok(gen_list)
}

/// 尝试锁定文件或超时
async fn lock_or_time_out(path: &PathBuf) -> KernelResult<LockFile> {
    let mut lock_file = LockFile::open(path)?;

    let mut backoff = 1;

    loop {
        if lock_file.try_lock()? {
            return Ok(lock_file);
        }
        if backoff > 4 {
            return Err(KernelError::ProcessExists);
        }
        time::sleep(Duration::from_millis(backoff * 100)).await;
        backoff *= 2;
    }
}


#[async_trait]
pub trait Storage: Send + Sync + 'static + Sized {
    /// 获取内核名
    fn name() -> &'static str
    where
        Self: Sized;

    /// 通过数据目录路径开启数据库
    async fn open(path: impl Into<PathBuf> + Send) -> KernelResult<Self>;

    /// 强制将数据刷入硬盘
    async fn flush(&self) -> KernelResult<()>;

    /// 设置键值对
    async fn set(&self, key: Bytes, value: Bytes) -> KernelResult<()>;

    /// 通过键获取对应的值
    async fn get(&self, key: &[u8]) -> KernelResult<Option<Bytes>>;

    /// 通过键删除键值对
    async fn remove(&self, key: &[u8]) -> KernelResult<()>;

    async fn size_of_disk(&self) -> KernelResult<u64>;

    async fn len(&self) -> KernelResult<usize>;

    async fn is_empty(&self) -> bool;
}




/// 基于LSM的KV Store存储内核
/// Leveled Compaction压缩算法
pub struct PiggyKV {
    pub(crate) inner: Arc<StoreInner>,
    /// 多进程文件锁
    /// 避免多进程进行数据读写
    lock_file: LockFile,
    /// Compactor 通信器
    pub(crate) compactor_tx: Sender<CompactTask>,
}



#[async_trait]
impl Storage for PiggyKV {
    #[inline]
    fn name() -> &'static str
    where
        Self: Sized,
    {
        "KipDB"
    }

    #[inline]
    async fn open(path: impl Into<PathBuf> + Send) -> KernelResult<Self> {
        PiggyKV::open_with_config(Config::new(path.into())).await
    }

    #[inline]
    async fn flush(&self) -> KernelResult<()> {
        let (tx, rx) = oneshot::channel();

        self.compactor_tx.send(CompactTask::Flush(Some(tx))).await?;

        rx.await.map_err(|_| KernelError::ChannelClose)?;

        Ok(())
    }

    #[inline]
    async fn set(&self, key: Bytes, value: Bytes) -> KernelResult<()> {
        self.append_cmd_data((key, Some(value))).await
    }

    #[inline]
    async fn get(&self, key: &[u8]) -> KernelResult<Option<Bytes>> {
        if let Some((_, value)) = self.mem_table().find(key) {
            return Ok(value);
        }

        let version = self.current_version().await;
        if let Some((_, value)) = query_and_compaction(key, &version, &self.compactor_tx)? {
            return Ok(value);
        }

        Ok(None)
    }

    #[inline]
    async fn remove(&self, key: &[u8]) -> KernelResult<()> {
        match self.get(key).await? {
            Some(_) => {
                self.append_cmd_data((Bytes::copy_from_slice(key), None))
                    .await
            }
            None => Err(KernelError::KeyNotFound),
        }
    }

    #[inline]
    async fn size_of_disk(&self) -> KernelResult<u64> {
        Ok(self.current_version().await.size_of_disk())
    }

    #[inline]
    async fn len(&self) -> KernelResult<usize> {
        Ok(self.current_version().await.len() + self.mem_table().len())
    }

    #[inline]
    async fn is_empty(&self) -> bool {
        self.current_version().await.is_empty() && self.mem_table().is_empty()
    }
}

impl Drop for PiggyKV {
    #[inline]
    #[allow(clippy::expect_used, clippy::let_underscore_must_use)]
    fn drop(&mut self) {
        self.lock_file.unlock().expect("LockFile unlock failed!");

        let _ = self.compactor_tx.try_send(CompactTask::Flush(None));
    }
}

impl PiggyKV {
    /// 追加数据
    async fn append_cmd_data(&self, data: KeyValue) -> KernelResult<()> {
        if self.mem_table().insert_data(data)? {
            if let Err(TrySendError::Closed(_)) =
                self.compactor_tx.try_send(CompactTask::Flush(None))
            {
                return Err(KernelError::ChannelClose);
            }
        }

        Ok(())
    }

    /// 使用Config进行LsmStore初始化
    #[inline]
    pub async fn open_with_config(config: Config) -> KernelResult<Self>
    where
        Self: Sized,
    {
        Gen::init();
        // 若lockfile的文件夹路径不存在则创建
        fs::create_dir_all(&config.dir_path)?;
        let lock_file = lock_or_time_out(&config.path().join(DEFAULT_LOCK_FILE)).await?;
        let inner = Arc::new(StoreInner::new(config.clone()).await?);
        let mut compactor = Compactor::new(Arc::clone(&inner));
        let (task_tx, mut task_rx) = channel(1);

        let _ignore = tokio::spawn(async move {
            while let Some(task) = task_rx.recv().await {
                match task {
                    CompactTask::Seek((scope, level)) => {
                        if let Err(err) =
                            compactor.major_compaction(level, scope, vec![], true).await
                        {
                            error!("[Compactor][manual compaction][error happen]: {:?}", err);
                        }
                    }
                    CompactTask::Flush(option_tx) => {
                        if let Err(err) = compactor.check_then_compaction(option_tx).await {
                            error!("[Compactor][compaction][error happen]: {:?}", err);
                        }
                    }
                }
            }
        });

        Ok(PiggyKV {
            inner,
            lock_file,
            compactor_tx: task_tx,
        })
    }

    pub(crate) fn mem_table(&self) -> &MemTable {
        &self.inner.mem_table
    }

    pub(crate) async fn current_version(&self) -> Arc<Version> {
        self.inner.ver_status.current().await
    }

    /// 创建事务
    #[inline]
    pub async fn new_transaction(&self, check_type: CheckType) -> Transaction {
        Transaction::new(self, check_type).await
    }

    #[inline]
    pub async fn manual_compaction(
        &self,
        min: Bytes,
        max: Bytes,
        level: usize,
    ) -> KernelResult<()> {
        if min <= max {
            self.compactor_tx
                .send(CompactTask::Seek((Scope::from_range(0, min, max), level)))
                .await?;
        }

        Ok(())
    }

    #[allow(dead_code)]
    async fn flush_background(&self) -> KernelResult<()> {
        self.compactor_tx.send(CompactTask::Flush(None)).await?;

        Ok(())
    }
}
