use bytes::Bytes;
use futures::future;
use itertools::Itertools;
use std::collections::HashSet;
use std::mem;
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::oneshot;
use tracing::info;

use crate::error::KernelError;
use crate::lsm::sstable::collect_gen;
use crate::lsm::MAX_LEVEL;
use crate::KernelResult;

use super::data_sharding;
use super::mem_table::{KeyValue, MemTable};
use super::sstable::meta::TableMeta;
use super::sstable::scope::Scope;
use super::sstable::Table;
use super::storage::{Config, StoreInner};
use super::version::edit::VersionEdit;
use super::version::status::VersionStatus;

pub(crate) const LEVEL_0: usize = 0;

/// 数据分片集
/// 包含对应分片的Gen与数据
pub(crate) type MergeShardingVec = Vec<(i64, Vec<KeyValue>)>;
pub(crate) type DelNode = (Vec<i64>, TableMeta);
/// Major压缩时的待删除Gen封装(N为此次Major所压缩的Level)，第一个为Level N级，第二个为Level N+1级
pub(crate) type DelNodeTuple = (DelNode, DelNode);
pub type SeekScope = (Scope, usize);

/// Store与Compactor的交互信息
#[derive(Debug)]
pub enum CompactTask {
    Seek(SeekScope),
    Flush(Option<oneshot::Sender<()>>),
}

/// 压缩器
///
/// 负责Minor和Major压缩
pub(crate) struct Compactor {
    store_inner: Arc<StoreInner>,
}

impl Compactor {
    pub(crate) fn new(store_inner: Arc<StoreInner>) -> Self {
        Compactor { store_inner }
    }

    /// 检查并进行压缩 （默认为 异步、被动 的Lazy压缩）
    ///
    /// 默认为try检测是否超出阈值，主要思路为以被动定时检测的机制使
    /// 多事务的commit脱离Compactor的耦合，
    /// 同时减少高并发事务或写入时的频繁Compaction，优先写入后统一压缩，
    /// 减少Level 0热数据的SSTable的冗余数据
    pub(crate) async fn check_then_compaction(
        &mut self,
        option_tx: Option<oneshot::Sender<()>>,
    ) -> KernelResult<()> {
        if let Some((gen, values)) = self.mem_table().swap()? {
            if !values.is_empty() {
                let start = Instant::now();
                // 目前minor触发major时是同步进行的，所以此处对live_tag是在此方法体保持存活
                self.minor_compaction(gen, values).await?;
                info!("[Compactor][Compaction Drop][Time: {:?}]", start.elapsed());
            }
        }

        // 压缩请求响应
        if let Some(tx) = option_tx {
            tx.send(()).map_err(|_| KernelError::ChannelClose)?
        }

        Ok(())
    }

    /// 持久化immutable_table为SSTable
    ///
    /// 请注意：vec_values必须是依照key值有序的
    pub(crate) async fn minor_compaction(
        &self,
        gen: i64,
        values: Vec<KeyValue>,
    ) -> KernelResult<()> {
        if !values.is_empty() {
            let (scope, meta) = self
                .ver_status()
                .loader()
                .create(
                    gen,
                    values,
                    LEVEL_0,
                    self.config().level_table_type[LEVEL_0],
                )
                .await?;

            // `Compactor::data_loading_with_level`中会检测是否达到压缩阈值，因此此处直接调用Major压缩
            self.major_compaction(
                LEVEL_0,
                scope.clone(),
                vec![VersionEdit::NewFile((vec![scope], 0), 0, meta)],
                false,
            )
            .await?;
        }
        Ok(())
    }

    /// Major压缩，负责将不同Level之间的数据向下层压缩转移
    /// 目前Major压缩的大体步骤是
    /// 1. 获取当前Version，通过传入的指定Scope得到该Level与该scope相交的SSTable，命名为tables_l
    /// 2. 获取的tables_l向下一级Level进行类似第2步骤的措施，获取两级之间压缩范围内最恰当的数据(table_ll的范围应当左右应包含与table_l)
    /// 3. tables_l与tables_ll之间的数据并行取出排序归并去重等处理后，分片成多个Vec<KeyValue>
    /// 4. 并行将每个分片各自生成SSTable
    /// 5. 生成的SSTables插入到tables_ll的第一个SSTable位置，并将tables_l和tables_ll的SSTable删除
    /// 6. 将变更的SSTable插入至vec_ver_edit以持久化
    /// Final: 将vec_ver_edit中的数据进行log_and_apply生成新的Version作为最新状态
    ///
    /// 经过压缩测试，Level 1的SSTable总是较多，根据原理推断：
    /// Level0的Key基本是无序的，容易生成大量的SSTable至Level1
    /// 而Level1-MAX_LEVEL的Key排布有序，故转移至下一层的SSTable数量较小
    /// 因此大量数据压缩的情况下Level 1的SSTable数量会较多
    pub(crate) async fn major_compaction(
        &self,
        mut level: usize,
        scope: Scope,
        mut vec_ver_edit: Vec<VersionEdit>,
        mut is_skip_sized: bool,
    ) -> KernelResult<()> {
        let config = self.config();
        let mut is_over = false;

        if level > MAX_LEVEL - 1 {
            return Err(KernelError::LevelOver);
        }

        while level < MAX_LEVEL && !is_over {
            let next_level = level + 1;

            // Tips: is_skip_sized选项仅仅允许跳过一次
            if let Some((
                index,
                ((del_gens_l, del_meta_l), (del_gens_ll, del_meta_ll)),
                vec_sharding,
            )) = self
                .data_loading_with_level(level, &scope, mem::replace(&mut is_skip_sized, false))
                .await?
            {
                let start = Instant::now();
                // 并行创建SSTable
                let table_futures = vec_sharding.into_iter().map(|(gen, sharding)| {
                    self.ver_status().loader().create(
                        gen,
                        sharding,
                        next_level,
                        config.level_table_type[next_level],
                    )
                });
                let vec_table_and_scope: Vec<(Scope, TableMeta)> =
                    future::try_join_all(table_futures).await?;
                let (new_scopes, new_metas): (Vec<Scope>, Vec<TableMeta>) =
                    vec_table_and_scope.into_iter().unzip();
                let fusion_meta = TableMeta::fusion(&new_metas);

                vec_ver_edit.append(&mut vec![
                    VersionEdit::NewFile((new_scopes, next_level), index, fusion_meta),
                    VersionEdit::DeleteFile((del_gens_l, level), del_meta_l),
                    VersionEdit::DeleteFile((del_gens_ll, next_level), del_meta_ll),
                ]);
                info!(
                    "[LsmStore][Major Compaction][recreate_sst][Level: {}][Time: {:?}]",
                    level,
                    start.elapsed()
                );
                level += 1;
            } else {
                is_over = true;
            }
            if !vec_ver_edit.is_empty() {
                self.ver_status()
                    .log_and_apply(
                        mem::take(&mut vec_ver_edit),
                        config.ver_log_snapshot_threshold,
                    )
                    .await?;
            }
        }
        Ok(())
    }

    /// 通过Level进行归并数据加载
    async fn data_loading_with_level(
        &self,
        level: usize,
        target: &Scope,
        is_skip_sized: bool,
    ) -> KernelResult<Option<(usize, DelNodeTuple, MergeShardingVec)>> {
        let version = self.ver_status().current().await;
        let config = self.config();
        let next_level = level + 1;

        // 如果该Level的SSTables数量尚未越出阈值则提取返回空
        if level > MAX_LEVEL - 2
            || !(is_skip_sized || version.is_threshold_exceeded_major(config, level))
        {
            return Ok(None);
        }

        // 此处vec_table_l指此level的Vec<SSTable>, vec_table_ll则是下一级的Vec<SSTable>
        // 类似罗马数字
        let start = Instant::now();

        // 获取此级中有重复键值范围的SSTable
        let (tables_l, scopes_l, _) = version.tables_by_scopes(level, target);
        if scopes_l.is_empty() {
            return Ok(None);
        }

        // 因此使用tables_l向下检测冲突时获取的集合应当含有tables_ll的元素
        let fusion_scope_l = Scope::fusion(&scopes_l).unwrap_or(target.clone());
        // 通过tables_l的scope获取下一级的父集
        let (tables_ll, _, index) = version.tables_by_scopes(next_level, &fusion_scope_l);

        // 收集需要清除的SSTable
        let del_gen_l = collect_gen(&tables_l)?;
        let del_gen_ll = collect_gen(&tables_ll)?;

        // 数据合并并切片
        let vec_merge_sharding =
            Self::data_merge_and_sharding(tables_l, tables_ll, config.sst_file_size).await?;
        info!(
            "[LsmStore][Major Compaction][data_loading_with_level][Time: {:?}]",
            start.elapsed()
        );

        Ok(Some((index, (del_gen_l, del_gen_ll), vec_merge_sharding)))
    }

    /// 以SSTables的数据归并再排序后切片，获取以KeyValue的Key值由小到大的切片排序
    /// 1. 并行获取Level l(当前等级)的待合并SSTables_l的全量数据
    /// 2. 基于SSTables_l获取唯一KeySet用于迭代过滤
    /// 3. 并行对Level ll的SSTables_ll通过KeySet进行迭代同时过滤数据
    /// 4. 组合SSTables_l和SSTables_ll的数据合并并进行唯一，排序处理
    #[allow(clippy::mutable_key_type)]
    async fn data_merge_and_sharding(
        tables_l: Vec<&dyn Table>,
        tables_ll: Vec<&dyn Table>,
        file_size: usize,
    ) -> KernelResult<MergeShardingVec> {
        // SSTables的Gen会基于时间有序生成,所有以此作为SSTables的排序依据
        let map_futures_l = tables_l
            .iter()
            .sorted_unstable_by_key(|table| table.gen())
            .map(|table| async { Self::table_load_data(table, |_| true) });

        let sharding_l = future::try_join_all(map_futures_l).await?;

        // 获取Level l的唯一KeySet用于Level ll的迭代过滤数据
        let filter_set_l: HashSet<&Bytes> = sharding_l
            .iter()
            .flatten()
            .map(|key_value| &key_value.0)
            .collect();

        // 通过KeySet过滤出Level l中需要补充的数据
        // 并行: 因为即使l为0时，此时的ll(Level 1)仍然保证SSTable数据之间排列有序且不冲突，因此并行迭代不会导致数据冲突
        // 过滤: 基于l进行数据过滤避免冗余的数据迭代导致占用大量内存占用
        let sharding_ll = future::try_join_all(tables_ll.iter().map(|table| async {
            Self::table_load_data(table, |key| !filter_set_l.contains(key))
        }))
        .await?;

        // 使用sharding_ll来链接sharding_l以保持数据倒序的顺序是由新->旧
        let vec_cmd_data = sharding_ll
            .into_iter()
            .chain(sharding_l)
            .flatten()
            .rev()
            .unique_by(|(key, _)| key.clone())
            .sorted_unstable_by_key(|(key, _)| key.clone())
            .collect();
        Ok(data_sharding(vec_cmd_data, file_size))
    }

    fn table_load_data<F>(table: &&dyn Table, fn_is_filter: F) -> KernelResult<Vec<KeyValue>>
    where
        F: Fn(&Bytes) -> bool,
    {
        let mut iter = table.iter()?;
        let mut vec_cmd = Vec::with_capacity(table.len());
        while let Some(item) = iter.try_next()? {
            if fn_is_filter(&item.0) {
                vec_cmd.push(item)
            }
        }
        Ok(vec_cmd)
    }

    pub(crate) fn config(&self) -> &Config {
        &self.store_inner.config
    }

    pub(crate) fn mem_table(&self) -> &MemTable {
        &self.store_inner.mem_table
    }

    pub(crate) fn ver_status(&self) -> &VersionStatus {
        &self.store_inner.ver_status
    }
}
