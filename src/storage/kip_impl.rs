// use crate::catalog::{ColumnCatalog, ColumnRef, TableCatalog, TableName};
// use crate::expression::simplify::ConstantBinary;
// use crate::storage::table_codec::TableCodec;
// use crate::storage::{
//     tuple_projection, Bounds, Iter, Projections, Storage, StorageError, Transaction,
// };
// use crate::types::errors::TypeError;
// use crate::types::index::{Index, IndexMetaRef};
// use crate::types::tuple::{Tuple, TupleId};
// use crate::types::value::ValueRef;
// use crate::types::ColumnId;
// use kip_db::kernel::lsm::iterator::Iter as KipDBIter;
// use kip_db::kernel::lsm::mvcc::{CheckType, TransactionIter};
// use kip_db::kernel::lsm::storage::Config;
// use kip_db::kernel::lsm::{mvcc, storage};
// use kip_db::kernel::utils::lru_cache::ShardingLruCache;
// use kip_db::KernelError;
// use std::collections::hash_map::RandomState;
// use std::collections::{Bound, VecDeque};
// use std::mem;
// use std::ops::SubAssign;
// use std::path::PathBuf;
// use std::sync::Arc;

// use super::IndexValue;
// // TODO: Table return optimization
// pub struct IndexIter<'a> {
//     offset: usize,
//     limit: Option<usize>,
//     projections: Projections,

//     index_meta: IndexMetaRef,
//     table: &'a TableCatalog,
//     tx: &'a mvcc::Transaction,

//     // for buffering data
//     index_values: VecDeque<IndexValue>,
//     binaries: VecDeque<ConstantBinary>,
//     scope_iter: Option<mvcc::TransactionIter<'a>>,
// }

// impl IndexIter<'_> {
//     fn offset_move(offset: &mut usize) -> bool {
//         if *offset > 0 {
//             offset.sub_assign(1);

//             true
//         } else {
//             false
//         }
//     }

//     fn val_to_key(&self, val: ValueRef) -> Result<Vec<u8>, TypeError> {
//         if self.index_meta.is_unique {
//             let index = Index::new(self.index_meta.id, vec![val]);

//             TableCodec::encode_index_key(&self.table.name, &index)
//         } else {
//             TableCodec::encode_tuple_key(&self.table.name, &val)
//         }
//     }

//     fn get_tuple_by_id(&mut self, tuple_id: &TupleId) -> Result<Option<Tuple>, StorageError> {
//         let key = TableCodec::encode_tuple_key(&self.table.name, &tuple_id)?;

//         self.tx
//             .get(&key)?
//             .map(|bytes| {
//                 let tuple = TableCodec::decode_tuple(self.table.all_columns(), &bytes);

//                 tuple_projection(&mut self.limit, &self.projections, tuple)
//             })
//             .transpose()
//     }

//     fn is_empty(&self) -> bool {
//         self.scope_iter.is_none() && self.index_values.is_empty() && self.binaries.is_empty()
//     }
// }

// impl Iter for IndexIter<'_> {
//     fn next_tuple(&mut self) -> Result<Option<Tuple>, StorageError> {
//         // 1. check limit
//         if matches!(self.limit, Some(0)) || self.is_empty() {
//             self.scope_iter = None;
//             self.binaries.clear();

//             return Ok(None);
//         }
//         // 2. try get tuple on index_values and until it empty
//         loop {
//             if let Some(value) = self.index_values.pop_front() {
//                 if Self::offset_move(&mut self.offset) {
//                     continue;
//                 }
//                 match value {
//                     IndexValue::PrimaryKey(tuple) => {
//                         let tuple = tuple_projection(&mut self.limit, &self.projections, tuple)?;

//                         return Ok(Some(tuple));
//                     }
//                     IndexValue::Normal(tuple_id) => {
//                         if let Some(tuple) = self.get_tuple_by_id(&tuple_id)? {
//                             return Ok(Some(tuple));
//                         }
//                     }
//                 }
//             } else {
//                 break;
//             }
//         }
//         assert!(self.index_values.is_empty());

//         // 3. If the current expression is a Scope,
//         // an iterator will be generated for reading the IndexValues of the Scope.
//         if let Some(iter) = &mut self.scope_iter {
//             let mut has_next = false;
//             while let Some((_, value_option)) = iter.try_next()? {
//                 if let Some(value) = value_option {
//                     if self.index_meta.is_primary {
//                         let tuple = TableCodec::decode_tuple(self.table.all_columns(), &value);

//                         self.index_values.push_back(IndexValue::PrimaryKey(tuple));
//                     } else {
//                         for tuple_id in TableCodec::decode_index(&value)? {
//                             self.index_values.push_back(IndexValue::Normal(tuple_id));
//                         }
//                     }
//                     has_next = true;
//                     break;
//                 }
//             }
//             if !has_next {
//                 self.scope_iter = None;
//             }
//             return self.next_tuple();
//         }

//         // 4. When `scope_iter` and `index_values` do not have a value, use the next expression to iterate
//         if let Some(binary) = self.binaries.pop_front() {
//             match binary {
//                 ConstantBinary::Scope { min, max } => {
//                     let table_name = &self.table.name;
//                     let index_meta = &self.index_meta;

//                     let bound_encode = |bound: Bound<ValueRef>| -> Result<_, StorageError> {
//                         match bound {
//                             Bound::Included(val) => Ok(Bound::Included(self.val_to_key(val)?)),
//                             Bound::Excluded(val) => Ok(Bound::Excluded(self.val_to_key(val)?)),
//                             Bound::Unbounded => Ok(Bound::Unbounded),
//                         }
//                     };
//                     let check_bound = |value: &mut Bound<Vec<u8>>, bound: Vec<u8>| {
//                         if matches!(value, Bound::Unbounded) {
//                             let _ = mem::replace(value, Bound::Included(bound));
//                         }
//                     };
//                     let (bound_min, bound_max) = if index_meta.is_unique {
//                         TableCodec::index_bound(table_name, &index_meta.id)
//                     } else {
//                         TableCodec::tuple_bound(table_name)
//                     };

//                     let mut encode_min = bound_encode(min)?;
//                     check_bound(&mut encode_min, bound_min);

//                     let mut encode_max = bound_encode(max)?;
//                     check_bound(&mut encode_max, bound_max);

//                     let iter = self.tx.iter(
//                         encode_min.as_ref().map(Vec::as_slice),
//                         encode_max.as_ref().map(Vec::as_slice),
//                     )?;
//                     self.scope_iter = Some(iter);
//                 }
//                 ConstantBinary::Eq(val) => {
//                     let key = self.val_to_key(val)?;
//                     if let Some(bytes) = self.tx.get(&key)? {
//                         if self.index_meta.is_unique {
//                             for tuple_id in TableCodec::decode_index(&bytes)? {
//                                 self.index_values.push_back(IndexValue::Normal(tuple_id));
//                             }
//                         } else if self.index_meta.is_primary {
//                             let tuple = TableCodec::decode_tuple(self.table.all_columns(), &bytes);

//                             self.index_values.push_back(IndexValue::PrimaryKey(tuple));
//                         } else {
//                             todo!()
//                         }
//                     }
//                     self.scope_iter = None;
//                 }
//                 _ => (),
//             }
//         }
//         self.next_tuple()
//     }
// }

// #[derive(Clone)]
// pub struct KipStorage {
//     pub inner: Arc<storage::KipStorage>,
// }

// impl KipStorage {
//     pub async fn new(path: impl Into<PathBuf> + Send) -> Result<Self, StorageError> {
//         let storage =
//             storage::KipStorage::open_with_config(Config::new(path).enable_level_0_memorization())
//                 .await?;

//         Ok(KipStorage {
//             inner: Arc::new(storage),
//         })
//     }
// }

// impl Storage for KipStorage {
//     type TransactionType = KipTransaction;

//     async fn transaction(&self) -> Result<Self::TransactionType, StorageError> {
//         let tx = self.inner.new_transaction(CheckType::Optimistic).await;

//         Ok(KipTransaction {
//             tx,
//             cache: ShardingLruCache::new(8, 2, RandomState::default())?,
//         })
//     }
// }

// pub struct KipTransaction {
//     tx: mvcc::Transaction,
//     cache: ShardingLruCache<String, TableCatalog>,
// }

// impl Transaction for KipTransaction {
//     type IterType<'a> = KipIter<'a>;
//     type IndexIterType<'a>=IndexIter<'a>;

//     fn read(
//         &self,
//         table_name: TableName,
//         bounds: Bounds,
//         projections: Projections,
//     ) -> Result<Self::IterType<'_>, StorageError> {
//         let all_columns = self
//             .table(table_name.clone())
//             .ok_or(StorageError::TableNotFound)?
//             .all_columns();
//         let (min, max) = TableCodec::tuple_bound(&table_name);
//         let iter = self.tx.iter(Bound::Included(&min), Bound::Included(&max))?;

//         Ok(KipIter {
//             offset: bounds.0.unwrap_or(0),
//             limit: bounds.1,
//             projections,
//             all_columns,
//             iter,
//         })
//     }

//     fn read_by_index(
//         &self,
//         table_name: TableName,
//         (offset_option, limit_option): Bounds,
//         projections: Projections,
//         index_meta: IndexMetaRef,
//         binaries: Vec<ConstantBinary>,
//     ) -> Result<Self::IndexIterType<'_>, StorageError> {
//         let table = self
//             .table(table_name.clone())
//             .ok_or(StorageError::TableNotFound)?;
//         let offset = offset_option.unwrap_or(0);

//         Ok(IndexIter {
//             offset,
//             limit: limit_option,
//             projections,
//             index_meta,
//             table,
//             index_values: VecDeque::new(),
//             binaries: VecDeque::from(binaries),
//             tx: &self.tx,
//             scope_iter: None,
//         })
//     }

//     fn add_index(
//         &mut self,
//         table_name: &str,
//         index: Index,
//         tuple_ids: Vec<TupleId>,
//         is_unique: bool,
//     ) -> Result<(), StorageError> {
//         let (key, value) = TableCodec::encode_index(table_name, &index, &tuple_ids)?;

//         if let Some(bytes) = self.tx.get(&key)? {
//             if is_unique {
//                 let old_tuple_ids = TableCodec::decode_index(&bytes)?;

//                 if old_tuple_ids[0] != tuple_ids[0] {
//                     return Err(StorageError::DuplicateUniqueValue);
//                 } else {
//                     return Ok(());
//                 }
//             } else {
//                 todo!("联合索引")
//             }
//         }

//         self.tx.set(key, value);

//         Ok(())
//     }

//     fn del_index(&mut self, table_name: &str, index: &Index) -> Result<(), StorageError> {
//         let key = TableCodec::encode_index_key(table_name, index)?;

//         self.tx.remove(&key)?;

//         Ok(())
//     }

//     fn append(
//         &mut self,
//         table_name: &str,
//         tuple: Tuple,
//         is_overwrite: bool,
//     ) -> Result<(), StorageError> {
//         let (key, value) = TableCodec::encode_tuple(table_name, &tuple)?;

//         if !is_overwrite && self.tx.get(&key)?.is_some() {
//             return Err(StorageError::DuplicatePrimaryKey);
//         }
//         self.tx.set(key, value);

//         Ok(())
//     }

//     fn delete(&mut self, table_name: &str, tuple_id: TupleId) -> Result<(), StorageError> {
//         let key = TableCodec::encode_tuple_key(table_name, &tuple_id)?;
//         self.tx.remove(&key)?;

//         Ok(())
//     }

//     fn add_column(
//         &mut self,
//         table_name: &TableName,
//         column: &ColumnCatalog,
//         if_not_exists: bool,
//     ) -> Result<ColumnId, StorageError> {
//         if let Some(mut catalog) = self.table(table_name.clone()).cloned() {
//             if !column.nullable && column.default_value().is_none() {
//                 return Err(StorageError::NeedNullAbleOrDefault);
//             }

//             for col in catalog.all_columns() {
//                 if col.name() == column.name() {
//                     if if_not_exists {
//                         return Ok(col.id().unwrap());
//                     } else {
//                         return Err(StorageError::DuplicateColumn);
//                     }
//                 }
//             }

//             let col_id = catalog.add_column(column.clone())?;

//             if column.desc.is_unique {
//                 let meta_ref = catalog.add_index_meta(
//                     format!("uk_{}", column.name()),
//                     vec![col_id],
//                     true,
//                     false,
//                 );
//                 let (key, value) = TableCodec::encode_index_meta(table_name, meta_ref)?;
//                 self.tx.set(key, value);
//             }

//             let column = catalog.get_column_by_id(&col_id).unwrap();
//             let (key, value) = TableCodec::encode_column(&table_name, column)?;
//             self.tx.set(key, value);
//             self.cache.remove(table_name);

//             Ok(col_id)
//         } else {
//             Err(StorageError::TableNotFound)
//         }
//     }

//     fn drop_column(
//         &mut self,
//         table_name: &TableName,
//         column_name: &str,
//         if_exists: bool,
//     ) -> Result<(), StorageError> {
//         if let Some(catalog) = self.table(table_name.clone()).cloned() {
//             let column = catalog.get_column_by_name(column_name).unwrap();

//             if let Some(index_meta) = catalog.get_unique_index(&column.id().unwrap()) {
//                 let (index_meta_key, _) = TableCodec::encode_index_meta(table_name, index_meta)?;
//                 self.tx.remove(&index_meta_key)?;

//                 let (index_min, index_max) = TableCodec::index_bound(table_name, &index_meta.id);
//                 Self::_drop_data(&mut self.tx, &index_min, &index_max)?;
//             }
//             let (key, _) = TableCodec::encode_column(&table_name, column)?;

//             match self.tx.remove(&key) {
//                 Ok(_) => (),
//                 Err(KernelError::KeyNotFound) => {
//                     if !if_exists {
//                         Err(KernelError::KeyNotFound)?;
//                     }
//                 }
//                 err => err?,
//             }
//             self.cache.remove(table_name);

//             Ok(())
//         } else {
//             Err(StorageError::TableNotFound)
//         }
//     }

//     fn create_table(
//         &mut self,
//         table_name: TableName,
//         columns: Vec<ColumnCatalog>,
//         if_not_exists: bool,
//     ) -> Result<TableName, StorageError> {
//         let (table_key, value) = TableCodec::encode_root_table(&table_name)?;
//         if self.tx.get(&table_key)?.is_some() {
//             if if_not_exists {
//                 return Ok(table_name);
//             }
//             return Err(StorageError::TableExists);
//         }
//         self.tx.set(table_key, value);

//         let mut table_catalog = TableCatalog::new(table_name.clone(), columns)?;

//         Self::create_index_meta_for_table(&mut self.tx, &mut table_catalog)?;

//         for column in table_catalog.columns.values() {
//             let (key, value) = TableCodec::encode_column(&table_name, column)?;
//             self.tx.set(key, value);
//         }
//         self.cache.put(table_name.to_string(), table_catalog);

//         Ok(table_name)
//     }

//     fn drop_table(&mut self, table_name: &str, if_exists: bool) -> Result<(), StorageError> {
//         if self.table(Arc::new(table_name.to_string())).is_none() {
//             if if_exists {
//                 return Ok(());
//             } else {
//                 return Err(StorageError::TableNotFound);
//             }
//         }
//         self.drop_data(table_name)?;

//         let (column_min, column_max) = TableCodec::columns_bound(table_name);
//         Self::_drop_data(&mut self.tx, &column_min, &column_max)?;

//         let (index_meta_min, index_meta_max) = TableCodec::index_meta_bound(table_name);
//         Self::_drop_data(&mut self.tx, &index_meta_min, &index_meta_max)?;

//         self.tx
//             .remove(&TableCodec::encode_root_table_key(table_name))?;

//         let _ = self.cache.remove(&table_name.to_string());

//         Ok(())
//     }

//     fn drop_data(&mut self, table_name: &str) -> Result<(), StorageError> {
//         let (tuple_min, tuple_max) = TableCodec::tuple_bound(table_name);
//         Self::_drop_data(&mut self.tx, &tuple_min, &tuple_max)?;

//         let (index_min, index_max) = TableCodec::all_index_bound(table_name);
//         Self::_drop_data(&mut self.tx, &index_min, &index_max)?;

//         Ok(())
//     }

//     fn table(&self, table_name: TableName) -> Option<&TableCatalog> {
//         let mut option = self.cache.get(&table_name);

//         if option.is_none() {
//             // TODO: unify the data into a `Meta` prefix and use one iteration to collect all data
//             let columns = Self::column_collect(table_name.clone(), &self.tx).ok()?;
//             let indexes = Self::index_meta_collect(&table_name, &self.tx)?;

//             if let Ok(catalog) =
//                 TableCatalog::new_with_indexes(table_name.clone(), columns, indexes)
//             {
//                 option = self
//                     .cache
//                     .get_or_insert(table_name.to_string(), |_| Ok(catalog))
//                     .ok();
//             }
//         }

//         option
//     }

//     fn show_tables(&self) -> Result<Vec<String>, StorageError> {
//         let mut tables = vec![];
//         let (min, max) = TableCodec::root_table_bound();
//         let mut iter = self.tx.iter(Bound::Included(&min), Bound::Included(&max))?;

//         while let Some((_, value_option)) = iter.try_next().ok().flatten() {
//             if let Some(value) = value_option {
//                 let table_name = TableCodec::decode_root_table(&value)?;

//                 tables.push(table_name);
//             }
//         }

//         Ok(tables)
//     }

//     async fn commit(self) -> Result<(), StorageError> {
//         self.tx.commit().await?;

//         Ok(())
//     }

//     async fn rollback(self) -> Result<(), StorageError> {
//         todo!()
//     }

// }

// impl KipTransaction {
//     fn column_collect(
//         table_name: TableName,
//         tx: &mvcc::Transaction,
//     ) -> Result<Vec<ColumnCatalog>, StorageError> {
//         let (column_min, column_max) = TableCodec::columns_bound(&table_name);
//         let mut column_iter =
//             tx.iter(Bound::Included(&column_min), Bound::Included(&column_max))?;

//         let mut columns = vec![];

//         while let Some((_, value_option)) = column_iter.try_next().ok().flatten() {
//             if let Some(value) = value_option {
//                 columns.push(TableCodec::decode_column(&value)?);
//             }
//         }

//         Ok(columns)
//     }

//     fn index_meta_collect(name: &str, tx: &mvcc::Transaction) -> Option<Vec<IndexMetaRef>> {
//         let (index_min, index_max) = TableCodec::index_meta_bound(name);
//         let mut index_metas = vec![];
//         let mut index_iter = tx
//             .iter(Bound::Included(&index_min), Bound::Included(&index_max))
//             .ok()?;

//         while let Some((_, value_option)) = index_iter.try_next().ok().flatten() {
//             if let Some(value) = value_option {
//                 if let Ok(index_meta) = TableCodec::decode_index_meta(&value) {
//                     index_metas.push(Arc::new(index_meta));
//                 }
//             }
//         }

//         Some(index_metas)
//     }

//     fn _drop_data(tx: &mut mvcc::Transaction, min: &[u8], max: &[u8]) -> Result<(), StorageError> {
//         let mut iter = tx.iter(Bound::Included(min), Bound::Included(max))?;
//         let mut data_keys = vec![];

//         while let Some((key, value_option)) = iter.try_next()? {
//             if value_option.is_some() {
//                 data_keys.push(key);
//             }
//         }
//         drop(iter);

//         for key in data_keys {
//             tx.remove(&key)?
//         }

//         Ok(())
//     }

//     fn create_index_meta_for_table(
//         tx: &mut mvcc::Transaction,
//         table: &mut TableCatalog,
//     ) -> Result<(), StorageError> {
//         let table_name = table.name.clone();

//         for col in table
//             .all_columns()
//             .into_iter()
//             .filter(|col| col.desc.is_index())
//         {
//             let is_primary = col.desc.is_primary;
//             // FIXME: composite indexes may exist on future
//             let prefix = if is_primary { "pk" } else { "uk" };

//             if let Some(col_id) = col.id() {
//                 let meta_ref = table.add_index_meta(
//                     format!("{}_{}", prefix, col.name()),
//                     vec![col_id],
//                     col.desc.is_unique,
//                     is_primary,
//                 );
//                 let (key, value) = TableCodec::encode_index_meta(&table_name, meta_ref)?;

//                 tx.set(key, value);
//             }
//         }
//         Ok(())
//     }
// }

// pub struct KipIter<'a> {
//     offset: usize,
//     limit: Option<usize>,
//     projections: Projections,
//     all_columns: Vec<ColumnRef>,
//     iter: TransactionIter<'a>,
// }

// impl Iter for KipIter<'_> {
//     fn next_tuple(&mut self) -> Result<Option<Tuple>, StorageError> {
//         while self.offset > 0 {
//             let _ = self.iter.try_next()?;
//             self.offset -= 1;
//         }

//         if let Some(num) = self.limit {
//             if num == 0 {
//                 return Ok(None);
//             }
//         }

//         while let Some(item) = self.iter.try_next()? {
//             if let (_, Some(value)) = item {
//                 let tuple = tuple_projection(
//                     &mut self.limit,
//                     &self.projections,
//                     TableCodec::decode_tuple(self.all_columns.clone(), &value),
//                 )?;

//                 return Ok(Some(tuple));
//             }
//         }

//         Ok(None)
//     }
// }

// impl From<KernelError> for StorageError {
//     fn from(value: KernelError) -> Self {
//         StorageError::KipDBError(value)
//     }
// }
