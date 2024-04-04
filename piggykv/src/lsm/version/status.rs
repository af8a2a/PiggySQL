use crate::io::{FileExtension, IoFactory, IoType, IoWriter};
use crate::lsm::wal::{LogLoader, LogWriter};
use crate::lsm::sstable::loader::TableLoader;
use crate::lsm::storage::{Config, Gen};
use crate::lsm::version::cleaner::Cleaner;
use crate::lsm::version::edit::VersionEdit;
use crate::lsm::version::{
    snapshot_gen, Version, DEFAULT_SS_TABLE_PATH, DEFAULT_VERSION_PATH,
};
use crate::KernelResult;
use std::mem;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use tokio::sync::mpsc::unbounded_channel;
use tokio::sync::RwLock;
use tracing::info;

/// 用于切换Version的封装Inner
struct VersionInner {
    version: Arc<Version>,
    ver_log_writer: (LogWriter<Box<dyn IoWriter>>, i64),
}

pub(crate) struct VersionStatus {
    inner: RwLock<VersionInner>,
    ss_table_loader: Arc<TableLoader>,
    log_factory: Arc<IoFactory>,
    edit_approximate_count: AtomicUsize,
}

impl VersionStatus {
    pub(crate) fn load_with_path(config: Config, wal: LogLoader) -> KernelResult<Self> {
        let sst_path = config.path().join(DEFAULT_SS_TABLE_PATH);
        let sst_factory = Arc::new(IoFactory::new(sst_path, FileExtension::SSTable)?);
        let ss_table_loader = Arc::new(TableLoader::new(
            config.clone(),
            Arc::clone(&sst_factory),
            wal,
        )?);
        let log_factory = Arc::new(IoFactory::new(
            config.path().join(DEFAULT_VERSION_PATH),
            FileExtension::Log,
        )?);
        let mut version_logs = Vec::new();
        let (ver_log_loader, log_gen) = LogLoader::reload(
            config.path(),
            (DEFAULT_VERSION_PATH, Some(snapshot_gen(&log_factory)?)),
            IoType::Direct,
            &mut version_logs,
            |bytes, records| {
                records.append(&mut bincode::deserialize::<Vec<VersionEdit>>(bytes)?);

                Ok(())
            },
        )?;
        let edit_approximate_count = AtomicUsize::new(version_logs.len());
        let (clean_tx, clean_rx) = unbounded_channel();
        let version = Arc::new(Version::load_from_log(
            version_logs,
            &ss_table_loader,
            clean_tx,
        )?);
        let mut cleaner = Cleaner::new(&ss_table_loader, clean_rx);

        let _ignore = tokio::spawn(async move {
            cleaner.listen().await;
        });

        let mut ver_log_writer = ver_log_loader.writer(log_gen)?;
        let _ = ver_log_writer.seek_end()?;

        Ok(Self {
            inner: RwLock::new(VersionInner {
                version,
                ver_log_writer: ((ver_log_writer), log_gen),
            }),
            ss_table_loader,
            log_factory,
            edit_approximate_count,
        })
    }

    pub(crate) async fn current(&self) -> Arc<Version> {
        Arc::clone(&self.inner.read().await.version)
    }

    /// 对一组VersionEdit持久化并应用
    pub(crate) async fn log_and_apply(
        &self,
        vec_version_edit: Vec<VersionEdit>,
        snapshot_threshold: usize,
    ) -> KernelResult<()> {
        let mut new_version = Version::clone(self.current().await.as_ref());
        let mut inner = self.inner.write().await;
        info!("[Version Status][log_and_apply]: {new_version}");

        if self.edit_approximate_count.load(Ordering::Relaxed) >= snapshot_threshold {
            Self::write_snap_shot(&mut inner, &self.log_factory).await?;
        } else {
            let _ = self.edit_approximate_count.fetch_add(1, Ordering::Relaxed);
        }

        let _ = inner
            .ver_log_writer
            .0
            .add_record(&bincode::serialize(&vec_version_edit)?)?;

        new_version.apply(vec_version_edit)?;
        inner.version = Arc::new(new_version);

        Ok(())
    }

    async fn write_snap_shot(
        inner: &mut VersionInner,
        log_factory: &IoFactory,
    ) -> KernelResult<()> {
        let version = &inner.version;
        info!(
            "[Version: {}][write_snap_shot]: Start Snapshot!",
            version.version_num
        );
        let new_gen = Gen::create();
        let new_writer = log_factory.writer(new_gen, IoType::Direct)?;
        let (mut old_writer, old_gen) = mem::replace(
            &mut inner.ver_log_writer,
            (LogWriter::new(new_writer), new_gen),
        );

        old_writer.flush()?;

        // 在快照中 append edit, 防止快照中宕机发生在删除旧 log 之后造成 增量 edit 未写入新log的问题
        let snap_shot_version_edits = version.to_vec_edit();
        let _ = inner
            .ver_log_writer
            .0
            .add_record(&bincode::serialize(&snap_shot_version_edits)?)?;

        // 删除旧的 version log
        log_factory.clean(old_gen)?;

        Ok(())
    }

    pub(crate) fn loader(&self) -> &TableLoader {
        &self.ss_table_loader
    }
}
