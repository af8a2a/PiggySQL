use bytes::Bytes;
use std::collections::hash_map::RandomState;
use std::io::Cursor;
use std::mem;
use std::sync::Arc;
use tracing::warn;

use crate::io::{IoFactory, IoType};
use crate::lsm::compactor::LEVEL_0;
use crate::lsm::mem_table::KeyValue;
use crate::lsm::sstable::btree_table::BTreeTable;
use crate::lsm::sstable::sst::block::{Entry, Value};
use crate::lsm::storage::Config;
use crate::lsm::wal::LogLoader;
use crate::utils::lru_cache::ShardingLruCache;
use crate::KernelResult;

use super::meta::TableMeta;
use super::scope::Scope;
use super::sst::block::BlockCache;
use super::sst::SSTable;
use super::{BoxTable, Table, TableType};

#[derive(Clone)]
pub(crate) struct TableLoader {
    inner: Arc<ShardingLruCache<i64, BoxTable>>,
    factory: Arc<IoFactory>,
    config: Config,
    wal: LogLoader,
    cache: Arc<BlockCache>,
}

impl TableLoader {
    pub(crate) fn new(
        config: Config,
        factory: Arc<IoFactory>,
        wal: LogLoader,
    ) -> KernelResult<Self> {
        let inner = Arc::new(ShardingLruCache::new(
            config.table_cache_size,
            16,
            RandomState::default(),
        )?);
        let cache = Arc::new(ShardingLruCache::new(
            config.block_cache_size,
            16,
            RandomState::default(),
        )?);
        Ok(TableLoader {
            inner,
            factory,
            config,
            wal,
            cache,
        })
    }

    #[allow(clippy::match_single_binding)]
    pub(crate) async fn create(
        &self,
        gen: i64,
        vec_data: Vec<KeyValue>,
        level: usize,
        table_type: TableType,
    ) -> KernelResult<(Scope, TableMeta)> {
        // 获取数据的Key涵盖范围
        let scope = Scope::from_sorted_vec_data(gen, &vec_data)?;
        let table: Box<dyn Table> = match table_type {
            TableType::SortedString => Box::new(self.create_ss_table(gen, vec_data, level).await?),
            TableType::BTree => Box::new(BTreeTable::new(level, gen, vec_data)),
        };
        let table_meta = TableMeta::from(table.as_ref());
        let _ = self.inner.put(gen, table);

        Ok((scope, table_meta))
    }

    pub(crate) fn get(&self, gen: i64) -> Option<&dyn Table> {
        self.inner
            .get_or_insert(gen, |gen| {
                let table_factory = &self.factory;

                let table: Box<dyn Table> = match table_factory
                    .reader(*gen, IoType::Direct)
                    .and_then(|reader| SSTable::load_from_file(reader, Arc::clone(&self.cache)))
                {
                    Ok(ss_table) => Box::new(ss_table),
                    Err(err) => {
                        // 尝试恢复仅对Level 0的Table有效
                        warn!(
                            "[LSMStore][Load Table: {}][try to reload with wal]: {:?}",
                            gen, err
                        );
                        let mut reload_data = Vec::new();
                        self.wal.load(*gen, &mut reload_data, |bytes, records| {
                            for (_, Entry { key, item, .. }) in
                                Entry::<Value>::batch_decode(&mut Cursor::new(mem::take(bytes)))?
                            {
                                records.push((key, item.bytes));
                            }

                            Ok(())
                        })?;

                        Box::new(BTreeTable::new(LEVEL_0, *gen, reload_data))
                    }
                };

                Ok(table)
            })
            .map(Box::as_ref)
            .ok()
    }

    async fn create_ss_table(
        &self,
        gen: i64,
        reload_data: Vec<(Bytes, Option<Bytes>)>,
        level: usize,
    ) -> KernelResult<SSTable> {
        SSTable::new(
            &self.factory,
            &self.config,
            Arc::clone(&self.cache),
            gen,
            reload_data,
            level,
            IoType::Direct,
        )
        .await
    }

    pub(crate) fn remove(&self, gen: &i64) -> Option<BoxTable> {
        self.inner.remove(gen)
    }

    #[allow(dead_code)]
    pub(crate) fn is_emtpy(&self) -> bool {
        self.inner.is_empty()
    }

    pub(crate) fn clean(&self, gen: i64) -> KernelResult<()> {
        let _ = self.remove(&gen);
        self.factory.clean(gen)?;
        self.wal.clean(gen)?;

        Ok(())
    }

    // Tips: 仅仅对持久化Table有效，SkipTable类内存Table始终为false
    #[allow(dead_code)]
    pub(crate) fn is_table_file_exist(&self, gen: i64) -> KernelResult<bool> {
        self.factory.exists(gen)
    }
}
