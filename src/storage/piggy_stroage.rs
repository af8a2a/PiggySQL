use std::mem;
use std::ops::Bound;
use std::sync::Arc;
use std::{collections::VecDeque, path::PathBuf};

use itertools::Itertools;
use moka::sync::Cache;
use piggykv::lsm::iterator::Iter;
use piggykv::lsm::mvcc::{CheckType, TransactionIter};
use piggykv::lsm::storage::Config;
use piggykv::lsm::{mvcc, PiggyKV};
use tracing::debug;

use crate::catalog::{ColumnCatalog, ColumnRef, IndexName};
use crate::catalog::{TableCatalog, TableName};
use crate::expression::simplify::ConstantBinary;

use crate::errors::*;
use crate::storage::table_codec::TableCodec;
use crate::types::index::{Index, IndexMeta, IndexMetaRef};
use crate::types::tuple::{Tuple, TupleId};
use crate::types::value::ValueRef;
use crate::types::{ColumnId, LogicalType};

use super::{tuple_projection, Bounds, Projections, Storage, StorageIter, Transaction};
pub struct PiggyKVStroage {
    db: Arc<PiggyKV>,
    cache: Arc<Cache<TableName, TableCatalog>>,
}
impl PiggyKVStroage {
    pub async fn new(path: impl Into<PathBuf> + Send) -> Result<Self> {
        let db = Arc::new(
            PiggyKV::open_with_config(Config::new(path).enable_level_0_memorization()).await?,
        );
        let cache = Arc::new(Cache::new(40));
        Ok(Self { db, cache })
    }
}
pub struct TransactionWarpper {
    txn: mvcc::Transaction,
    cache: Arc<Cache<TableName, TableCatalog>>,
}

impl Storage for PiggyKVStroage {
    type TransactionType = TransactionWarpper;

    async fn transaction(&self) -> Result<Self::TransactionType> {
        Ok(TransactionWarpper {
            txn: self.db.new_transaction(CheckType::Optimistic).await,
            cache: self.cache.clone(),
        })
    }
}
pub struct IndexIterator<'a> {
    projection: Projections,

    index_meta: IndexMetaRef,
    table: TableCatalog,
    txn: &'a mvcc::Transaction,
    // scope_iter: Option<mvcc::TransactionIter<'a>>,
    ranges: VecDeque<ConstantBinary>,
}

impl<'a> IndexIterator<'a> {
    fn val_to_key(&self, val: ValueRef) -> Result<Vec<u8>> {
        if self.index_meta.is_unique {
            let index = Index::new(self.index_meta.id, vec![val]);

            TableCodec::encode_index_key(&self.table.name, &index)
        } else {
            TableCodec::encode_tuple_key(&self.table.name, &val)
        }
    }
    fn get_tuple_by_id(&self, tuple_id: &TupleId) -> Result<Option<Tuple>> {
        let key = TableCodec::encode_tuple_key(&self.table.name, tuple_id)?;
        let schema = self.table.all_columns();
        self.txn
            .get(&key)
            .unwrap()
            .map(|bytes| {
                let tuple = TableCodec::decode_tuple(&schema, &bytes);

                tuple_projection(&self.projection, &schema, tuple)
            })
            .transpose()
    }
}
impl<'a> StorageIter for IndexIterator<'a> {
    fn fetch_tuple(&mut self) -> Result<Option<Vec<Tuple>>> {
        let mut tuples: Vec<Tuple> = Vec::new();
        let schema = self.table.all_columns();
        for binary in self.ranges.iter().cloned() {
            match binary {
                ConstantBinary::Scope { min, max } => {
                    let table_name = &self.table.name;
                    let index_meta = &self.index_meta;

                    let bound_encode = |bound: Bound<ValueRef>| -> Result<_> {
                        match bound {
                            Bound::Included(val) => Ok(Bound::Included(self.val_to_key(val)?)),
                            Bound::Excluded(val) => Ok(Bound::Excluded(self.val_to_key(val)?)),
                            Bound::Unbounded => Ok(Bound::Unbounded),
                        }
                    };
                    let check_bound = |value: &mut Bound<Vec<u8>>, bound: Vec<u8>| {
                        if matches!(value, Bound::Unbounded) {
                            let _ = mem::replace(value, Bound::Included(bound));
                        }
                    };
                    let (bound_min, bound_max) = if index_meta.is_unique {
                        TableCodec::index_bound(table_name, &index_meta.id)
                    } else {
                        TableCodec::tuple_bound(table_name)
                    };

                    let mut encode_min = bound_encode(min)?;
                    check_bound(&mut encode_min, bound_min);

                    let mut encode_max = bound_encode(max)?;
                    check_bound(&mut encode_max, bound_max);
                    let encode_min = match encode_min {
                        Bound::Included(ref lo) => Bound::Included(lo.as_slice()),
                        Bound::Excluded(ref lo) => Bound::Excluded(lo.as_slice()),
                        Bound::Unbounded => Bound::Unbounded,
                    };
                    let encode_max = match encode_max {
                        Bound::Included(ref lo) => Bound::Included(lo.as_slice()),
                        Bound::Excluded(ref lo) => Bound::Excluded(lo.as_slice()),
                        Bound::Unbounded => Bound::Unbounded,
                    };
                    let collect_iter = self.txn.iter(encode_min, encode_max)?;
                    if self.index_meta.is_primary {
                        //主键索引可以直接获得元组
                        let collect = collect_iter
                            .filter_map(|(_, v)| v)
                            .map(|v| -> Tuple { TableCodec::decode_tuple(&schema, &v) })
                            .collect_vec();
                        tuples.extend(collect);
                    } else {
                        let index_values = collect_iter
                            .filter_map(|(_, v)| v)
                            .map(|v| TableCodec::decode_index(&v).expect("decode index error"))
                            .collect_vec();
                        for tuple_ids in index_values {
                            for tuple_id in tuple_ids {
                                if let Some(tuple) = self.get_tuple_by_id(&tuple_id)? {
                                    tuples.push(tuple);
                                }
                            }
                        }
                    }
                }
                ConstantBinary::Eq(val) => {
                    let key = self.val_to_key(val)?;
                    if let Some(Ok(bytes)) = self.txn.get(&key).transpose() {
                        let mut index_values = Vec::new();

                        if self.index_meta.is_unique {
                            for tuple_id in TableCodec::decode_index(&bytes)? {
                                index_values.push(tuple_id);
                            }
                            for tuple_id in index_values {
                                if let Some(tuple) = self.get_tuple_by_id(&tuple_id)? {
                                    tuples.push(tuple);
                                }
                            }
                        } else if self.index_meta.is_primary {
                            let tuple = TableCodec::decode_tuple(&schema, &bytes);
                            tuples.push(tuple);
                        } else {
                            todo!()
                        }
                    }
                }
                _ => (),
            }
        }

        Ok(Some(tuples))
    }
}

pub struct PiggyIterator<'a> {
    projection: Projections,
    all_columns: Vec<ColumnRef>,
    offset: Option<usize>,
    limit: Option<usize>,
    iter: TransactionIter<'a>,
}
impl StorageIter for PiggyIterator<'_> {
    fn fetch_tuple(&mut self) -> Result<Option<Vec<Tuple>>> {
        let limit = match self.limit {
            Some(limit) => limit,
            None => usize::MAX,
        };
        let offset = self.offset.unwrap_or(0);
        // let mut tuples = vec![];
        let tuples = self
            .iter
            .by_ref()
            .skip(offset)
            .filter_map(|(_, v)| v)
            .filter_map(|val| {
                tuple_projection(
                    &self.projection,
                    &self.all_columns,
                    TableCodec::decode_tuple(&self.all_columns, &val),
                )
                .ok()
            })
            .take(limit)
            .collect_vec();
        println!("len: {}", tuples.len());

        Ok(Some(tuples))
    }
}

impl Transaction for TransactionWarpper {
    type IterType<'a> = PiggyIterator<'a>;

    type IndexIterType<'a> = IndexIterator<'a>;

    fn read(
        &self,
        table_name: TableName,
        bound: Bounds,
        projection: Projections,
    ) -> Result<Self::IterType<'_>> {
        let all_columns = self
            .table(table_name.clone())
            .ok_or(DatabaseError::TableNotFound)?
            .all_columns();

        let (min, max) = TableCodec::tuple_bound(&table_name);
        let iter = self
            .txn
            .iter(Bound::Included(&min), Bound::Included(&max))?;
        Ok(PiggyIterator {
            projection,
            all_columns,
            offset: bound.0,
            limit: bound.1,
            iter,
        })
    }

    fn read_by_index(
        &self,
        table_name: TableName,
        projection: Projections,
        index_meta: IndexMetaRef,
        binaries: Vec<ConstantBinary>,
    ) -> Result<Self::IndexIterType<'_>> {
        let table = self
            .table(table_name.clone())
            .ok_or(DatabaseError::TableNotFound)?;
        Ok(IndexIterator {
            projection,
            index_meta,
            table,
            ranges: VecDeque::from(binaries),
            txn: &self.txn,
            // scope_iter: None,
        })
    }

    fn add_index(
        &mut self,
        table_name: &str,
        index: Index,
        tuple_ids: Vec<TupleId>,
        is_unique: bool,
    ) -> Result<()> {
        let (key, value) = TableCodec::encode_index(table_name, &index, &tuple_ids)?;

        if let Some(bytes) = self.txn.get(&key).unwrap() {
            if is_unique {
                let old_tuple_ids = TableCodec::decode_index(&bytes)?;

                if old_tuple_ids[0] != tuple_ids[0] {
                    return Err(DatabaseError::DuplicateUniqueValue);
                } else {
                    return Ok(());
                }
            } else {
                todo!("联合索引")
            }
        }

        self.txn.set(key, value);

        Ok(())
    }

    fn del_index(&mut self, table_name: &str, index: &Index) -> Result<()> {
        let key = TableCodec::encode_index_key(table_name, index)?;

        self.txn.remove(&key)?;

        Ok(())
    }

    fn append(&mut self, table_name: &str, tuple: Tuple, is_overwrite: bool) -> Result<()> {
        let (key, value) = TableCodec::encode_tuple(table_name, &tuple)?;

        if !is_overwrite && self.txn.get(&key).unwrap().is_some() && tuple.id.is_some() {
            return Err(DatabaseError::DuplicatePrimaryKey);
        }
        self.txn.set(key, value);
        Ok(())
    }

    fn delete(&mut self, table_name: &str, tuple_id: TupleId) -> Result<()> {
        let key = TableCodec::encode_tuple_key(table_name, &tuple_id)?;
        self.txn.remove(&key)?;

        Ok(())
    }

    fn add_column(
        &mut self,
        table_name: &TableName,
        column: &ColumnCatalog,
        if_not_exists: bool,
    ) -> Result<ColumnId> {
        // if self.concurrency_transaction.load(Ordering::SeqCst) > 1 {
        //     return Err(DatabaseError::DDLSerialError(
        //         self.concurrency_transaction.load(Ordering::SeqCst),
        //     ));
        // }

        if let Some(mut catalog) = self.table(table_name.clone()) {
            if !column.nullable && column.default_value().is_none() {
                return Err(DatabaseError::NeedNullAbleOrDefault);
            }

            for col in catalog.all_columns() {
                if col.name() == column.name() {
                    if if_not_exists {
                        return Ok(col.id().unwrap());
                    } else {
                        return Err(DatabaseError::DuplicateColumn);
                    }
                }
            }

            let col_id = catalog.add_column(column.clone())?;

            if column.desc.is_unique {
                let meta_ref = catalog.add_index_meta(
                    format!("uk_{}", column.name()),
                    vec![col_id],
                    true,
                    false,
                );
                let (key, value) = TableCodec::encode_index_meta(table_name, meta_ref)?;
                self.txn.set(key, value);
            }

            let column = catalog.get_column_by_id(&col_id).unwrap();
            let (key, value) = TableCodec::encode_column(&table_name, column)?;
            self.txn.set(key, value);
            self.cache.remove(table_name);
            Ok(col_id)
        } else {
            Err(DatabaseError::TableNotFound)
        }
    }

    fn drop_column(&mut self, table_name: &TableName, column: &str, if_exists: bool) -> Result<()> {
        if let Some(catalog) = self.table(table_name.clone()) {
            let column = match catalog.get_column_by_name(column) {
                Some(col) => col,
                None => {
                    if if_exists {
                        return Ok(());
                    } else {
                        return Err(DatabaseError::NotFound(
                            "Coloum",
                            format!("{} not found", column),
                        ));
                    }
                }
            };

            if let Some(index_meta) = catalog.get_unique_index(&column.id().unwrap()) {
                let (index_meta_key, _) = TableCodec::encode_index_meta(table_name, index_meta)?;
                self.txn.remove(&index_meta_key)?;

                let (index_min, index_max) = TableCodec::index_bound(table_name, &index_meta.id);
                Self::_drop_data(&mut self.txn, &index_min, &index_max)?;
            }
            let (key, _) = TableCodec::encode_column(table_name, column)?;
            self.txn.remove(&key)?;
            self.cache.remove(table_name);
            Ok(())
        } else {
            Err(DatabaseError::TableNotFound)
        }
    }

    fn create_table(
        &mut self,
        table_name: TableName,
        columns: Vec<ColumnCatalog>,
        if_not_exists: bool,
    ) -> Result<TableName> {
        let (table_key, value) = TableCodec::encode_root_table(&table_name)?;
        if self.txn.get(&table_key).transpose().is_some() {
            if if_not_exists {
                return Ok(table_name);
            }
            return Err(DatabaseError::TableExists);
        }
        self.txn.set(table_key, value);

        let mut table_catalog = TableCatalog::new(table_name.clone(), columns)?;

        Self::create_primary_key(&mut self.txn, &mut table_catalog)?;
        Self::_create_index(&mut self.txn,&mut table_catalog, None)?;
        // println!("create_table:table_catalog: {:#?}", table_catalog);
        for column in table_catalog.columns.values() {
            let (key, value) = TableCodec::encode_column(&table_name, column)?;
            self.txn.set(key, value);
        }
        // info!("create_table:table_catalog: {:#?}", table_catalog);
        self.cache.insert(table_name.clone(), table_catalog);

        Ok(table_name)
    }

    fn drop_table(&mut self, table_name: &str, if_exists: bool) -> Result<()> {
        if self.table(Arc::new(table_name.to_string())).is_none() {
            if if_exists {
                return Ok(());
            } else {
                return Err(DatabaseError::TableNotFound);
            }
        }
        self.drop_data(table_name)?;

        let (column_min, column_max) = TableCodec::columns_bound(table_name);
        Self::_drop_data(&mut self.txn, &column_min, &column_max)?;

        let (index_meta_min, index_meta_max) = TableCodec::index_meta_bound(table_name);
        Self::_drop_data(&mut self.txn, &index_meta_min, &index_meta_max)?;

        self.txn
            .remove(&TableCodec::encode_root_table_key(table_name))?;
        self.cache.remove(&Arc::new(table_name.to_string()));
        Ok(())
    }

    fn drop_data(&mut self, table_name: &str) -> Result<()> {
        //删除元组数据
        let (tuple_min, tuple_max) = TableCodec::tuple_bound(table_name);
        Self::_drop_data(&mut self.txn, &tuple_min, &tuple_max)?;
        //删除关联索引数据
        let (index_min, index_max) = TableCodec::all_index_bound(table_name);
        Self::_drop_data(&mut self.txn, &index_min, &index_max)?;

        Ok(())
    }

    fn table(&self, table_name: TableName) -> Option<TableCatalog> {
        match self.cache.get(&table_name) {
            Some(table) => Some(table),
            None => {
                // debug!("cache:{:?}",self.cache);
                let columns = match self.column_collect(table_name.clone()) {
                    Ok(cols) => cols,
                    Err(e) => {
                        debug!("cannot fetch table {},because:{}", table_name, e);
                        return None;
                    }
                };

                let indexes = Self::index_meta_collect(&self.txn, &table_name)?
                    .into_iter()
                    .map(Arc::new)
                    .collect_vec();
                //todo
                match TableCatalog::new_with_indexes(table_name.clone(), columns, indexes) {
                    Ok(table) => {
                        self.cache.insert(table_name, table.clone());
                        Some(table)
                    }
                    Err(e) => {
                        debug!("cannot fetch table {},because:{}", table_name, e);
                        None
                    }
                }
            }
        }
    }

    fn show_tables(&self) -> Result<Vec<String>> {
        let (min, max) = TableCodec::root_table_bound();
        let scan = self
            .txn
            .iter(Bound::Included(&min), Bound::Included(&max))?;

        let metas = scan
            .filter_map(|(_, v)| v)
            .filter_map(|val| TableCodec::decode_root_table(&val).ok())
            .collect_vec();
        Ok(metas)
    }

    async fn commit(self) -> Result<()> {
        self.txn.commit().await?;
        Ok(())
    }

    async fn rollback(self) -> Result<()> {
        Ok(())
    }

    fn set_isolation(&mut self, _serializable: bool) -> Result<()> {
        todo!()
    }

    fn create_index(
        &mut self,
        table_name: TableName,
        index_name: IndexName,
        column_name: &str,
    ) -> Result<()> {
        let indexs = Self::index_meta_collect(&self.txn, &table_name).unwrap_or_default();
        let indexs = indexs.into_iter().map(Arc::new).collect_vec();
        let mut cols = self.column_collect(table_name.clone())?;
        let col = cols.iter_mut().find(|col| col.name() == column_name);
        if let Some(col) = col {
            col.desc.is_unique = true;
            let mut table = TableCatalog::new_with_indexes(table_name.clone(), cols, indexs)?;
            Self::_create_index(&mut self.txn,&mut table, Some(index_name.to_string()))?;
            self.cache.remove(&table_name);
        }
        Ok(())
    }

    fn drop_index(
        &mut self,
        table_name: TableName,
        index_name: IndexName,
        _if_not_exists: bool,
    ) -> Result<()> {
        //check index exists
        //operator in copy temp data
        let mut indexs = Self::index_meta_collect(&self.txn, &table_name).unwrap();
        let (i, _) = indexs
            .iter()
            .find_position(|meta| meta.name == format!("{}_{}", "uk", index_name))
            .unwrap();
        let item = indexs.remove(i);
        let mut cols = self.column_collect(table_name.clone()).unwrap();
        let indexs = indexs.into_iter().map(Arc::new).collect_vec();
        cols.get_mut(item.column_ids[0] as usize)
            .and_then(|col| Some(col.desc.is_unique = false));
        //更新索引元数据
        //todo
        //这是一个相当愚蠢的更新方法，受限于tablecodec的设计,我们必须先获取表的全部索引元信息,全部删除后再添加
        //这会造成相当大的IO写入
        let (index_meta_min, index_meta_max) = TableCodec::index_meta_bound(&table_name);
        Self::_drop_data(&mut self.txn, &index_meta_min, &index_meta_max)?;
        for meta in indexs.iter() {
            let (key, value) = TableCodec::encode_index_meta(&table_name, meta)?;
            self.txn.set(key, value);
        }
        //删除索引数据
        let (index_min, index_max) = TableCodec::index_bound(&table_name, &item.id);
        Self::_drop_data(&mut self.txn, &index_min, &index_max)?;

        let table = TableCatalog::new_with_indexes(table_name.clone(), cols, indexs)?;
        Self::update_table_meta(&mut self.txn, &table)?;
        self.cache.remove(&table_name);

        Ok(())
    }
}

impl TransactionWarpper {
    fn _drop_data(tx: &mut mvcc::Transaction, min: &[u8], max: &[u8]) -> Result<()> {
        let mut iter = tx.iter(Bound::Included(min), Bound::Included(max))?;
        let mut data_keys = vec![];

        while let Some((key, value_option)) = iter.try_next()? {
            if value_option.is_some() {
                data_keys.push(key);
            }
        }
        drop(iter);

        for key in data_keys {
            tx.remove(&key)?
        }
        Ok(())
    }
    fn update_table_meta(tx: &mut mvcc::Transaction, table: &TableCatalog) -> Result<()> {
        for column in table.columns.values() {
            let (key, value) = TableCodec::encode_column(&table.name, column)?;
            tx.set(key, value);
        }
        Ok(())
    }

    ///获取一个表关联的所有索引
    fn index_meta_collect(tx: &mvcc::Transaction, name: &str) -> Option<Vec<IndexMeta>> {
        let (index_min, index_max) = TableCodec::index_meta_bound(name);

        let scan = tx
            .iter(Bound::Included(&index_min), Bound::Included(&index_max))
            .unwrap();
        // let mut index_iter = index_scan.iter();
        let index_metas = scan
            .filter_map(|(_, val)| val)
            .filter_map(|val| TableCodec::decode_index_meta(&val).ok())
            .collect_vec();

        Some(index_metas)
    }
    ///获取一个表关联的列信息
    fn column_collect(&self, table_name: TableName) -> Result<Vec<ColumnCatalog>> {
        let (column_min, column_max) = TableCodec::columns_bound(&table_name);
        let scan = self
            .txn
            .iter(Bound::Included(&column_min), Bound::Included(&column_max))?;
        let columns = scan
            .filter_map(|(_, val)| val)
            .filter_map(|value| TableCodec::decode_column(&value).ok())
            .collect_vec();

        Ok(columns)
    }
    fn create_primary_key(tx: &mut mvcc::Transaction, table: &mut TableCatalog) -> Result<()> {
        let table_name = table.name.clone();

        let index_column = table
            .all_columns()
            .into_iter()
            .filter(|col| col.desc.is_primary)
            .collect_vec();

        for col in index_column {
            // FIXME: composite indexes may exist on future
            let prefix = "pk";
            if let Some(col_id) = col.id() {
                let meta_ref = table.add_index_meta(
                    format!("{}_{}", prefix, col.name()),
                    vec![col_id],
                    col.desc.is_unique,
                    col.desc.is_primary,
                );
                // println!("{:#?}", meta_ref);

                let (key, value) = TableCodec::encode_index_meta(&table_name, meta_ref)?;

                tx.set(key, value);
            }
        }
        Ok(())
    }

    fn _create_index(tx: &mut mvcc::Transaction, table: &mut TableCatalog, index_name: Option<String>) -> Result<()> {
        let table_name = table.name.clone();

        for col in table
            .all_columns()
            .into_iter()
            .filter(|col| col.desc.is_unique)
        {
            let mut name = col.name().to_string();
            if let Some(index_name) = &index_name {
                name = index_name.clone();
            }
            // FIXME: composite indexes may exist on future
            let prefix = "uk";
            if let Some(col_id) = col.id() {
                let meta_ref = table.add_index_meta(
                    format!("{}_{}", prefix, name),
                    vec![col_id],
                    col.desc.is_unique,
                    col.desc.is_primary,
                );
                // println!("meta_ref:{:#?}", meta_ref);

                let (key, value) = TableCodec::encode_index_meta(&table_name, meta_ref)?;

                tx.set(key, value);
            }
        }
        Ok(())
    }
}
#[cfg(test)]
mod test {

    use crate::{
        catalog::ColumnDesc,
        expression::ScalarExpression,
        types::{value::DataValue, LogicalType},
    };

    use super::*;
    #[tokio::test]
    async fn test_in_storage() -> Result<()> {
        let path = tempdir::TempDir::new("piggydb")
            .unwrap()
            .path()
            .join("piggydb");

        let storage = PiggyKVStroage::new(path).await?;
        let mut transaction = storage.transaction().await?;
        let columns = vec![
            Arc::new(ColumnCatalog::new(
                "c1".to_string(),
                false,
                ColumnDesc::new(LogicalType::Integer, true, false, None),
                // None,
            )),
            Arc::new(ColumnCatalog::new(
                "c2".to_string(),
                false,
                ColumnDesc::new(LogicalType::Boolean, false, false, None),
                // None,
            )),
        ];
        let source_columns = columns
            .iter()
            .map(|col_ref| ColumnCatalog::clone(&col_ref))
            .collect_vec();
        let _ = transaction.create_table(Arc::new("test".to_string()), source_columns, false)?;
        let table_catalog = transaction.table(Arc::new("test".to_string()));
        assert!(table_catalog.is_some());
        let cols = table_catalog.unwrap().all_columns();

        transaction.append(
            &"test".to_string(),
            Tuple {
                id: Some(Arc::new(DataValue::Int32(Some(1)))),
                values: vec![
                    Arc::new(DataValue::Int32(Some(1))),
                    Arc::new(DataValue::Boolean(Some(true))),
                ],
            },
            false,
        )?;
        transaction.append(
            &"test".to_string(),
            Tuple {
                id: Some(Arc::new(DataValue::Int32(Some(2)))),
                values: vec![
                    Arc::new(DataValue::Int32(Some(2))),
                    Arc::new(DataValue::Boolean(Some(false))),
                ],
            },
            false,
        )?;
        let mut iter = transaction.read(
            Arc::new("test".to_string()),
            (None, None),
            vec![ScalarExpression::ColumnRef(cols[0].clone())],
        )?;

        let tuples = iter.fetch_tuple()?;
        println!("{:#?}", tuples);

        if let Some(tuples) = tuples {
            assert_eq!(tuples[0].id, Some(Arc::new(DataValue::Int32(Some(1)))));
            assert_eq!(tuples[0].values, vec![Arc::new(DataValue::Int32(Some(1))),],);
            assert_eq!(tuples[1].id, Some(Arc::new(DataValue::Int32(Some(2)))));
            assert_eq!(tuples[1].values, vec![Arc::new(DataValue::Int32(Some(2))),],);
        }
        Ok(())
    }
}
