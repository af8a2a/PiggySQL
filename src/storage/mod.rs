pub mod engine;
pub mod mvcc;
mod table_codec;

use itertools::Itertools;
use moka::sync::Cache;

use crate::catalog::{ColumnCatalog, ColumnRef, IndexName, TableCatalog, TableName};

use crate::errors::*;
use crate::expression::simplify::ConstantBinary;
use crate::expression::ScalarExpression;
use crate::storage::table_codec::TableCodec;
use crate::types::index::{Index, IndexMeta, IndexMetaRef};
use crate::types::tuple::{Tuple, TupleId};
use crate::types::value::ValueRef;
use crate::types::ColumnId;
use std::collections::{Bound,  VecDeque};
use std::mem;
use std::sync::{Arc};

use self::engine::memory::Memory;
use self::engine::StorageEngine;
use self::mvcc::{Scan, MVCC};
pub trait Storage: Sync + Send {
    type TransactionType: Transaction;

    #[allow(async_fn_in_trait)]
    async fn transaction(&self) -> Result<Self::TransactionType>;
}

/// Optional bounds of the reader, of the form (offset, limit).
pub(crate) type Bounds = (Option<usize>, Option<usize>);
type Projections = Vec<ScalarExpression>;

pub trait Transaction: Sync + Send + 'static {
    type IterType<'a>: Iter;
    type IndexIterType<'a>: Iter;

    /// The bounds is applied to the whole data batches, not per batch.
    ///
    /// The projections is column indices.
    fn read(&self, table_name: TableName, projection: Projections) -> Result<Self::IterType<'_>>;

    fn read_by_index(
        &self,
        table_name: TableName,
        projection: Projections,
        index_meta: IndexMetaRef,
        binaries: Vec<ConstantBinary>,
    ) -> Result<Self::IndexIterType<'_>>;

    fn add_index(
        &mut self,
        table_name: &str,
        index: Index,
        tuple_ids: Vec<TupleId>,
        is_unique: bool,
    ) -> Result<()>;

    fn del_index(&mut self, table_name: &str, index: &Index) -> Result<()>;

    fn append(&mut self, table_name: &str, tuple: Tuple, is_overwrite: bool) -> Result<()>;

    fn delete(&mut self, table_name: &str, tuple_id: TupleId) -> Result<()>;

    fn add_column(
        &mut self,
        table_name: &TableName,
        column: &ColumnCatalog,
        if_not_exists: bool,
    ) -> Result<ColumnId>;

    fn drop_column(&mut self, table_name: &TableName, column: &str, if_exists: bool) -> Result<()>;

    fn create_table(
        &mut self,
        table_name: TableName,
        columns: Vec<ColumnCatalog>,
        if_not_exists: bool,
    ) -> Result<TableName>;

    fn drop_table(&mut self, table_name: &str, if_exists: bool) -> Result<()>;
    fn drop_data(&mut self, table_name: &str) -> Result<()>;
    fn table(&self, table_name: TableName) -> Option<TableCatalog>;

    fn show_tables(&self) -> Result<Vec<String>>;

    #[allow(async_fn_in_trait)]
    fn commit(self) -> Result<()>;

    #[allow(async_fn_in_trait)]
    fn rollback(self) -> Result<()>;

    fn create_index(
        &mut self,
        table_name: TableName,
        index_name: IndexName,
        column_name: &str,
    ) -> Result<()>;
    fn drop_index(
        &mut self,
        table_name: TableName,
        index_name: IndexName,
        if_not_exists: bool,
    ) -> Result<()>;
}

enum IndexValue {
    PrimaryKey(Tuple),
    Normal(TupleId),
}

pub trait Iter: Sync + Send {
    fn fetch_tuple(&mut self) -> Result<Option<Vec<Tuple>>>;
}

pub(crate) fn tuple_projection(projections: &Projections, tuple: Tuple) -> Result<Tuple> {
    let projection_len = projections.len();
    let mut columns = Vec::with_capacity(projection_len);
    let mut values = Vec::with_capacity(projection_len);

    for expr in projections.iter() {
        values.push(expr.eval(&tuple)?);
        columns.push(expr.output_columns());
    }
    Ok(Tuple {
        id: tuple.id,
        columns,
        values,
    })
}

pub struct MVCCIter<'a, E: StorageEngine> {
    projection: Projections,
    all_columns: Vec<ColumnRef>,
    scan: Scan<'a, E>,
}

impl<E: StorageEngine> Iter for MVCCIter<'_, E> {
    fn fetch_tuple(&mut self) -> Result<Option<Vec<Tuple>>> {
        let tuples = self
            .scan
            .iter()
            .filter_map(|item| item.ok())
            .map(|(_, val)| {
                tuple_projection(
                    &self.projection,
                    TableCodec::decode_tuple(self.all_columns.clone(), &val),
                )
            })
            .collect::<Result<Vec<_>>>()?;
        // println!("scan collect tuple {}", tuples.len());
        Ok(Some(tuples))
    }
}

pub struct MVCCIndexIter<'a, E: StorageEngine> {
    projection: Projections,

    index_meta: IndexMetaRef,
    table: TableCatalog,
    tx: &'a mvcc::MVCCTransaction<E>,

    binaries: VecDeque<ConstantBinary>,
}

impl<E: StorageEngine> MVCCIndexIter<'_, E> {
    fn val_to_key(&self, val: ValueRef) -> Result<Vec<u8>> {
        if self.index_meta.is_unique {
            let index = Index::new(self.index_meta.id, vec![val]);

            TableCodec::encode_index_key(&self.table.name, &index)
        } else {
            TableCodec::encode_tuple_key(&self.table.name, &val)
        }
    }
    fn get_tuple_by_id(&self, tuple_id: &TupleId) -> Result<Option<Tuple>> {
        let key = TableCodec::encode_tuple_key(&self.table.name, &tuple_id)?;

        self.tx
            .get(&key)?
            .map(|bytes| {
                let tuple = TableCodec::decode_tuple(self.table.all_columns(), &bytes);

                tuple_projection(&self.projection, tuple)
            })
            .transpose()
    }
}

impl<E: StorageEngine> Iter for MVCCIndexIter<'_, E> {
    fn fetch_tuple(&mut self) -> Result<Option<Vec<Tuple>>> {
        let mut tuples = Vec::new();

        for binary in self.binaries.iter().cloned() {
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
                    let mut collect_iter = self.tx.scan(encode_min, encode_max)?;
                    if self.index_meta.is_primary {
                        //主键索引可以直接获得元组
                        let collect = collect_iter
                            .iter()
                            .filter_map(|res| res.ok())
                            .map(|(_, v)| -> Tuple {
                                TableCodec::decode_tuple(self.table.all_columns(), &v)
                            })
                            .collect_vec();
                        tuples.extend(collect);
                    } else {
                        let index_values = collect_iter
                            .iter()
                            .filter_map(|res| res.ok())
                            .map(|(_, v)| TableCodec::decode_index(&v).expect("decode index error"))
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
                    if let Some(bytes) = self.tx.get(&key)? {
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
                            let tuple = TableCodec::decode_tuple(self.table.all_columns(), &bytes);
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

pub struct MVCCTransaction<E: StorageEngine> {
    tx: mvcc::MVCCTransaction<E>,
    cache:Arc<Cache<TableName,TableCatalog>>,
}
impl<E: StorageEngine> Transaction for MVCCTransaction<E> {
    type IterType<'a> = MVCCIter<'a, E>;

    type IndexIterType<'a> = MVCCIndexIter<'a, E>;

    fn read(&self, table_name: TableName, projection: Projections) -> Result<Self::IterType<'_>> {
        let all_columns = self
            .table(table_name.clone())
            .ok_or(DatabaseError::TableNotFound)?
            .all_columns();
        let (min, max) = TableCodec::tuple_bound(&table_name);
        Ok(MVCCIter {
            projection,
            all_columns,
            scan: self.tx.scan(Bound::Included(min), Bound::Included(max))?,
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
        Ok(MVCCIndexIter {
            projection,
            index_meta,
            table,
            binaries: VecDeque::from(binaries),
            tx: &self.tx,
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

        if let Some(bytes) = self.tx.get(&key)? {
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

        self.tx.set(&key, value.to_vec())?;

        Ok(())
    }

    fn del_index(&mut self, table_name: &str, index: &Index) -> Result<()> {
        let key = TableCodec::encode_index_key(table_name, index)?;

        self.tx.delete(&key)?;

        Ok(())
    }

    fn append(&mut self, table_name: &str, tuple: Tuple, is_overwrite: bool) -> Result<()> {
        let (key, value) = TableCodec::encode_tuple(table_name, &tuple)?;

        if !is_overwrite && self.tx.get(&key)?.is_some() {
            return Err(DatabaseError::DuplicatePrimaryKey);
        }
        self.tx.set(&key, value.to_vec())?;

        Ok(())
    }

    fn delete(&mut self, table_name: &str, tuple_id: TupleId) -> Result<()> {
        let key = TableCodec::encode_tuple_key(table_name, &tuple_id)?;
        self.tx.delete(&key)?;

        Ok(())
    }

    fn add_column(
        &mut self,
        table_name: &TableName,
        column: &ColumnCatalog,
        if_not_exists: bool,
    ) -> Result<ColumnId> {
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
                self.tx.set(&key, value.to_vec())?;
            }

            let column = catalog.get_column_by_id(&col_id).unwrap();
            let (key, value) = TableCodec::encode_column(&table_name, column)?;
            self.tx.set(&key, value.to_vec())?;
            self.cache.remove(table_name);
            Ok(col_id)
        } else {
            Err(DatabaseError::TableNotFound)
        }
    }

    fn drop_column(
        &mut self,
        table_name: &TableName,
        column: &str,
        _if_exists: bool,
    ) -> Result<()> {
        if let Some(catalog) = self.table(table_name.clone()) {
            let column = catalog.get_column_by_name(column).unwrap();

            if let Some(index_meta) = catalog.get_unique_index(&column.id().unwrap()) {
                let (index_meta_key, _) = TableCodec::encode_index_meta(table_name, index_meta)?;
                self.tx.delete(&index_meta_key)?;
                
                let (index_min, index_max) = TableCodec::index_bound(table_name, &index_meta.id);
                Self::_drop_data(&mut self.tx, &index_min, &index_max)?;
            }
            let (key, _) = TableCodec::encode_column(&table_name, column)?;

            match self.tx.delete(&key) {
                Ok(_) => (),
                //todo
                // Err(MVCCError::KeyError(e)) => {
                //     if !if_exists {
                //         Err(MVCCError::KeyError(e))?;
                //     }
                // }
                err => err?,
            }
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
        if self.tx.get(&table_key)?.is_some() {
            if if_not_exists {
                return Ok(table_name);
            }
            return Err(DatabaseError::TableExists);
        }
        self.tx.set(&table_key, value.to_vec())?;

        let mut table_catalog = TableCatalog::new(table_name.clone(), columns)?;

        Self::create_primary_key(&mut self.tx, &mut table_catalog)?;
        Self::create_index(&mut self.tx, &mut table_catalog, None)?;
        for column in table_catalog.columns.values() {
            let (key, value) = TableCodec::encode_column(&table_name, column)?;
            self.tx.set(&key, value.to_vec())?;
        }
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
        Self::_drop_data(&mut self.tx, &column_min, &column_max)?;

        let (index_meta_min, index_meta_max) = TableCodec::index_meta_bound(table_name);
        Self::_drop_data(&mut self.tx, &index_meta_min, &index_meta_max)?;

        self.tx
            .delete(&TableCodec::encode_root_table_key(table_name))?;
        self.cache.remove(&Arc::new(table_name.to_string()));
        Ok(())
    }
    ///删除一个表内所有数据
    fn drop_data(&mut self, table_name: &str) -> Result<()> {
        //删除元组数据
        let (tuple_min, tuple_max) = TableCodec::tuple_bound(table_name);
        Self::_drop_data(&mut self.tx, &tuple_min, &tuple_max)?;
        //删除关联索引数据
        let (index_min, index_max) = TableCodec::all_index_bound(table_name);
        Self::_drop_data(&mut self.tx, &index_min, &index_max)?;

        Ok(())
    }

    fn table(&self, table_name: TableName) -> Option<TableCatalog> {
        // TODO: unify the data into a `Meta` prefix and use one iteration to collect all data
        if self.cache.contains_key(&table_name){
            return self.cache.get(&table_name);
        }
        let columns = Self::column_collect(table_name.clone(), &self.tx).ok()?;
        let indexes = Self::index_meta_collect(&table_name, &self.tx)?
            .into_iter()
            .map(Arc::new)
            .collect_vec();

        match TableCatalog::new_with_indexes(table_name.clone(), columns, indexes) {
            Ok(table) => Some(table),
            Err(e) => {
                println!("{:#?}", e);
                None
            }
        }
    }

    fn show_tables(&self) -> Result<Vec<String>> {
        let mut metas = vec![];
        let (min, max) = TableCodec::root_table_bound();
        let mut scan = self.tx.scan(Bound::Included(min), Bound::Included(max))?;
        let mut iter = scan.iter();
        while let Some((_, value)) = iter.next().transpose()? {
            let meta = TableCodec::decode_root_table(&value)?;

            metas.push(meta);
        }

        Ok(metas)
    }

    fn commit(self) -> Result<()> {
        self.tx.commit()?;

        Ok(())
    }

    fn rollback(self) -> Result<()> {
        self.tx.rollback()?;

        Ok(())
    }

    fn create_index(
        &mut self,
        table_name: TableName,
        index_name: IndexName,
        column_name: &str,
    ) -> Result<()> {
        //todo error handling
        let indexs = Self::index_meta_collect(&table_name, &self.tx).unwrap_or_default();
        let indexs = indexs.into_iter().map(Arc::new).collect_vec();
        let mut cols = Self::column_collect(table_name.clone(), &self.tx)?;
        let col = cols.iter_mut().find(|col| col.name() == column_name);
        if let Some(col) = col {
            col.desc.is_unique = true;
            let mut table = TableCatalog::new_with_indexes(table_name.clone(), cols, indexs)?;
            Self::create_index(&mut self.tx, &mut table, Some(index_name.to_string()))?;
            self.cache.remove(&table_name);

        }
        Ok(())
    }

    fn drop_index(
        &mut self,
        table_name: TableName,
        index_name: IndexName,
        _if_exists: bool,
    ) -> Result<()> {
        //check index exists
        //operator in copy temp data
        let mut indexs = Self::index_meta_collect(&table_name, &self.tx).unwrap();
        let (i, _) = indexs
            .iter()
            .find_position(|meta| meta.name == format!("{}_{}", "uk", index_name.to_string()))
            .unwrap();
        let item = indexs.remove(i);
        let mut cols = Self::column_collect(table_name.clone(), &self.tx).unwrap();
        let indexs = indexs.into_iter().map(Arc::new).collect_vec();
        cols.get_mut(item.column_ids[0] as usize)
            .and_then(|col| Some(col.desc.is_unique = false));
        //更新索引元数据
        //todo
        //这是一个相当愚蠢的更新方法，受限于tablecodec的设计,我们必须先获取表的全部索引元信息,全部删除后再添加
        //这会造成相当大无意义的IO写入
        let (index_meta_min, index_meta_max) = TableCodec::index_meta_bound(&table_name);
        Self::_drop_data(&mut self.tx, &index_meta_min, &index_meta_max)?;
        for meta in indexs.iter() {
            let (key, value) = TableCodec::encode_index_meta(&table_name, &meta)?;
            self.tx.set(&key, value.to_vec())?;
        }
        //删除索引数据
        let (index_min, index_max) = TableCodec::index_bound(&table_name, &item.id);
        Self::_drop_data(&mut self.tx, &index_min, &index_max)?;

        let table = TableCatalog::new_with_indexes(table_name.clone(), cols, indexs)?;
        Self::update_table_meta(&mut self.tx, &table)?;
        self.cache.remove(&table_name);

        Ok(())
    }
}

impl<E: StorageEngine> MVCCTransaction<E> {
    fn update_table_meta(tx: &mvcc::MVCCTransaction<E>, table:&TableCatalog) -> Result<()> {
        for column in table.columns.values() {
            let (key, value) = TableCodec::encode_column(&table.name, column)?;
            tx.set(&key, value.to_vec())?;
        }
        Ok(())
    }

    fn create_primary_key(
        tx: &mut mvcc::MVCCTransaction<E>,
        table: &mut TableCatalog,
    ) -> Result<()> {
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

                tx.set(&key, value.to_vec())?;
            }
        }
        Ok(())
    }

    fn create_index(
        tx: &mut mvcc::MVCCTransaction<E>,
        table: &mut TableCatalog,
        index_name: Option<String>,
    ) -> Result<()> {
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

                tx.set(&key, value.to_vec())?;
            }
        }
        Ok(())
    }
    fn _drop_data(tx: &mut mvcc::MVCCTransaction<E>, min: &[u8], max: &[u8]) -> Result<()> {
        let mut scan = tx.scan(Bound::Included(min.to_vec()), Bound::Included(max.to_vec()))?;
        let mut iter = scan.iter();
        let mut data_keys = vec![];

        while let Ok(Some((key, _))) = iter.next().transpose() {
            data_keys.push(key);
        }

        for key in data_keys {
            tx.delete(&key)?
        }

        Ok(())
    }
    ///获取一个表关联的所有索引
    fn index_meta_collect(name: &str, tx: &mvcc::MVCCTransaction<E>) -> Option<Vec<IndexMeta>> {
        let (index_min, index_max) = TableCodec::index_meta_bound(name);
        let mut index_metas = vec![];
        let mut index_scan = tx
            .scan(Bound::Included(index_min), Bound::Included(index_max))
            .expect("scan index meta error");
        let mut index_iter = index_scan.iter();

        while let Ok(Some((_, value_option))) = index_iter.next().transpose() {
            if let Ok(index_meta) = TableCodec::decode_index_meta(&value_option) {
                index_metas.push(index_meta);
            }
        }
        //  println!("{:#?}", index_metas);
        Some(index_metas)
    }
    ///获取一个表关联的列信息
    fn column_collect(
        table_name: TableName,
        tx: &mvcc::MVCCTransaction<E>,
    ) -> Result<Vec<ColumnCatalog>> {
        let (column_min, column_max) = TableCodec::columns_bound(&table_name);
        let mut scan = tx.scan(Bound::Included(column_min), Bound::Included(column_max))?;
        let mut column_iter = scan.iter();
        let mut columns = vec![];

        while let Ok(Some((_, value))) = column_iter.next().transpose() {
            columns.push(TableCodec::decode_column(&value)?);
        }

        Ok(columns)
    }
}
#[derive(Clone)]
pub struct MVCCLayer<E: StorageEngine> {
    layer: mvcc::MVCC<E>,
    cache:Arc<Cache<TableName,TableCatalog>>,
}
impl<E: StorageEngine> MVCCLayer<E> {
    pub fn new(engine: E) -> Self {
        Self {
            layer: MVCC::new(Arc::new(engine)),
            cache: Arc::new(Cache::new(20)),
        }
    }
}
impl MVCCLayer<Memory> {
    pub fn new_memory() -> Self {
        Self {
            layer: MVCC::new(Arc::new(Memory::new())),
            cache: Arc::new(Cache::new(20)),
        }
    }
}
impl<E: StorageEngine> Storage for MVCCLayer<E> {
    type TransactionType = MVCCTransaction<E>;

    async fn transaction(&self) -> Result<Self::TransactionType> {
        Ok(MVCCTransaction {
            tx: self.layer.begin(false).await?,
            cache: Arc::clone(&self.cache),
        })
    }
}

#[cfg(test)]
mod test {

    use crate::{
        catalog::ColumnDesc,
        types::{value::DataValue, LogicalType},
    };

    use self::engine::memory::Memory;

    use super::*;
    #[tokio::test]
    async fn test_in_storage_works_with_data() -> Result<()> {
        let storage = MVCCLayer::new(Memory::new());
        let mut transaction = storage.transaction().await?;
        let columns = vec![
            Arc::new(ColumnCatalog::new(
                "c1".to_string(),
                false,
                ColumnDesc::new(LogicalType::Integer, true, false, None),
                None,
            )),
            Arc::new(ColumnCatalog::new(
                "c2".to_string(),
                false,
                ColumnDesc::new(LogicalType::Boolean, false, false, None),
                None,
            )),
        ];
        let source_columns = columns
            .iter()
            .map(|col_ref| ColumnCatalog::clone(&col_ref))
            .collect_vec();
        let _ = transaction.create_table(Arc::new("test".to_string()), source_columns, false)?;
        let table_catalog = transaction.table(Arc::new("test".to_string()));
        assert!(table_catalog.is_some());
        assert!(table_catalog
            .unwrap()
            .get_column_id_by_name(&"c1".to_string())
            .is_some());

        transaction.append(
            &"test".to_string(),
            Tuple {
                id: Some(Arc::new(DataValue::Int32(Some(1)))),
                columns: columns.clone(),
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
                columns: columns.clone(),
                values: vec![
                    Arc::new(DataValue::Int32(Some(2))),
                    Arc::new(DataValue::Boolean(Some(false))),
                ],
            },
            false,
        )?;
        let mut iter = transaction.read(
            Arc::new("test".to_string()),
            vec![ScalarExpression::ColumnRef(columns[0].clone())],
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
