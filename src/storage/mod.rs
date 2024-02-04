pub mod engine;
mod keycode;
pub mod mvcc;
mod table_codec;

use itertools::Itertools;

use crate::catalog::{CatalogError, ColumnCatalog, ColumnRef, TableCatalog, TableName};
use crate::expression::simplify::ConstantBinary;
use crate::expression::ScalarExpression;
use crate::storage::table_codec::TableCodec;
use crate::types::errors::TypeError;
use crate::types::index::{Index, IndexMetaRef};
use crate::types::tuple::{Tuple, TupleId};
use crate::types::value::ValueRef;
use crate::types::ColumnId;
use std::collections::{Bound, VecDeque};
use std::mem;
use std::ops::SubAssign;
use std::sync::Arc;

use self::engine::memory::Memory;
use self::engine::StorageEngine;
use self::mvcc::{MVCCError, Scan, MVCC};

pub trait Storage: Sync + Send {
    type TransactionType: Transaction;

    #[allow(async_fn_in_trait)]
    fn transaction(&self) -> Result<Self::TransactionType, StorageError>;
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
    fn read(
        &self,
        table_name: TableName,
        bounds: Bounds,
        projection: Projections,
    ) -> Result<Self::IterType<'_>, StorageError>;

    fn read_by_index(
        &self,
        table_name: TableName,
        bounds: Bounds,
        projection: Projections,
        index_meta: IndexMetaRef,
        binaries: Vec<ConstantBinary>,
    ) -> Result<Self::IndexIterType<'_>, StorageError>;

    fn add_index(
        &mut self,
        table_name: &str,
        index: Index,
        tuple_ids: Vec<TupleId>,
        is_unique: bool,
    ) -> Result<(), StorageError>;

    fn del_index(&mut self, table_name: &str, index: &Index) -> Result<(), StorageError>;

    fn append(
        &mut self,
        table_name: &str,
        tuple: Tuple,
        is_overwrite: bool,
    ) -> Result<(), StorageError>;

    fn delete(&mut self, table_name: &str, tuple_id: TupleId) -> Result<(), StorageError>;

    fn add_column(
        &mut self,
        table_name: &TableName,
        column: &ColumnCatalog,
        if_not_exists: bool,
    ) -> Result<ColumnId, StorageError>;

    fn drop_column(
        &mut self,
        table_name: &TableName,
        column: &str,
        if_exists: bool,
    ) -> Result<(), StorageError>;

    fn create_table(
        &mut self,
        table_name: TableName,
        columns: Vec<ColumnCatalog>,
        if_not_exists: bool,
    ) -> Result<TableName, StorageError>;

    fn drop_table(&mut self, table_name: &str, if_exists: bool) -> Result<(), StorageError>;
    fn drop_data(&mut self, table_name: &str) -> Result<(), StorageError>;
    fn table(&self, table_name: TableName) -> Option<TableCatalog>;

    fn show_tables(&self) -> Result<Vec<String>, StorageError>;

    #[allow(async_fn_in_trait)]
    fn commit(self) -> Result<(), StorageError>;

    #[allow(async_fn_in_trait)]
    fn rollback(self) -> Result<(), StorageError>;
}

enum IndexValue {
    PrimaryKey(Tuple),
    Normal(TupleId),
}

pub trait Iter: Sync + Send {
    fn fetch_tuple(&mut self) -> Result<Option<Vec<Tuple>>, StorageError>;
}

pub(crate) fn tuple_projection(
    limit: &mut Option<usize>,
    projections: &Projections,
    tuple: Tuple,
) -> Result<Tuple, StorageError> {
    let projection_len = projections.len();
    let mut columns = Vec::with_capacity(projection_len);
    let mut values = Vec::with_capacity(projection_len);

    for expr in projections.iter() {
        values.push(expr.eval(&tuple)?);
        columns.push(expr.output_columns());
    }

    if let Some(num) = limit {
        num.sub_assign(1);
    }

    Ok(Tuple {
        id: tuple.id,
        columns,
        values,
    })
}

#[derive(thiserror::Error, Debug)]
pub enum StorageError {
    #[error("catalog error")]
    CatalogError(#[from] CatalogError),

    #[error("type error")]
    TypeError(#[from] TypeError),

    #[error("The same primary key data already exists")]
    DuplicatePrimaryKey,

    #[error("The column has been declared unique and the value already exists")]
    DuplicateUniqueValue,

    #[error("The table not found")]
    TableNotFound,

    #[error("The some column already exists")]
    DuplicateColumn,

    #[error("Add column must be nullable or specify a default value")]
    NeedNullAbleOrDefault,

    #[error("The table already exists")]
    TableExists,

    #[error("MVCC layer error")]
    MvccLayerError(MVCCError),
}
impl From<MVCCError> for StorageError {
    fn from(value: MVCCError) -> Self {
        StorageError::MvccLayerError(value)
    }
}

pub struct MVCCIter<'a, E: StorageEngine> {
    offset: usize,
    limit: Option<usize>,
    projection: Projections,
    all_columns: Vec<ColumnRef>,
    scan: Scan<'a, E>,
}

impl<E: StorageEngine> Iter for MVCCIter<'_, E> {
    fn fetch_tuple(&mut self) -> Result<Option<Vec<Tuple>>, StorageError> {
        let mut iter = self.scan.iter();
        while self.offset > 0 {
            let _ = iter.next();
            self.offset -= 1;
        }
        if let Some(num) = self.limit {
            if num == 0 {
                return Ok(None);
            }
        }
        let tuples = iter
            .filter(|item| item.is_ok())
            .map(|item| {
                item.map_err(|e| StorageError::from(e))
                    .expect("unwarp item error")
            })
            .map(|(_, value)| {
                tuple_projection(
                    &mut self.limit,
                    &self.projection,
                    TableCodec::decode_tuple(self.all_columns.clone(), &value),
                )
                .expect("projection tuple error")
            })
            .collect_vec();
        Ok(Some(tuples))
    }
}
pub struct MVCCIndexIter<'a, E: StorageEngine> {
    offset: usize,
    limit: Option<usize>,
    projection: Projections,

    index_meta: IndexMetaRef,
    table: TableCatalog,
    tx: &'a mvcc::MVCCTransaction<E>,

    // for buffering data
    index_values: VecDeque<IndexValue>,
    binaries: VecDeque<ConstantBinary>,
    scope_iter: Option<Scan<'a, E>>,
}

impl<E: StorageEngine> MVCCIndexIter<'_, E> {
    fn offset_move(offset: &mut usize) -> bool {
        if *offset > 0 {
            offset.sub_assign(1);

            true
        } else {
            false
        }
    }
    fn val_to_key(&self, val: ValueRef) -> Result<Vec<u8>, TypeError> {
        if self.index_meta.is_unique {
            let index = Index::new(self.index_meta.id, vec![val]);

            TableCodec::encode_index_key(&self.table.name, &index)
        } else {
            TableCodec::encode_tuple_key(&self.table.name, &val)
        }
    }
    fn get_tuple_by_id(&mut self, tuple_id: &TupleId) -> Result<Option<Tuple>, StorageError> {
        let key = TableCodec::encode_tuple_key(&self.table.name, &tuple_id)?;

        self.tx
            .get(&key)?
            .map(|bytes| {
                let tuple = TableCodec::decode_tuple(self.table.all_columns(), &bytes);

                tuple_projection(&mut self.limit, &self.projection, tuple)
            })
            .transpose()
    }

    fn is_empty(&self) -> bool {
        self.scope_iter.is_none() && self.index_values.is_empty() && self.binaries.is_empty()
    }
}

impl<E: StorageEngine> Iter for MVCCIndexIter<'_, E> {
    fn fetch_tuple(&mut self) -> Result<Option<Vec<Tuple>>, StorageError> {
        let mut tuples = Vec::new();

        // 2. try get tuple on index_values and until it empty
        loop {
            if matches!(self.limit, Some(0)) || self.is_empty() {
                self.scope_iter = None;
                self.binaries.clear();

                break;
            }
            loop {
                if let Some(value) = self.index_values.pop_front() {
                    if Self::offset_move(&mut self.offset) {
                        continue;
                    }
                    match value {
                        IndexValue::PrimaryKey(tuple) => {
                            let tuple = tuple_projection(&mut self.limit, &self.projection, tuple)?;
                            tuples.push(tuple);
                        }
                        IndexValue::Normal(tuple_id) => {
                            if let Some(tuple) = self.get_tuple_by_id(&tuple_id)? {
                                tuples.push(tuple);
                            }
                        }
                    }
                } else {
                    break;
                }
            }
            assert!(self.index_values.is_empty());
            if let Some(iter) = &mut self.scope_iter {
                let mut iter = iter.iter();
                let mut has_next = false;

                while let Ok(Some((_, value))) = iter.next().transpose() {
                    if self.index_meta.is_primary {
                        let tuple = TableCodec::decode_tuple(self.table.all_columns(), &value);

                        self.index_values.push_back(IndexValue::PrimaryKey(tuple));
                    } else {
                        for tuple_id in TableCodec::decode_index(&value)? {
                            self.index_values.push_back(IndexValue::Normal(tuple_id));
                        }
                    }
                    has_next = true;
                    break;
                }
                drop(iter);
                if !has_next {
                    self.scope_iter = None;
                }
                continue;
                // return self.next_tuple();
            }
            // 4. When `scope_iter` and `index_values` do not have a value, use the next expression to iterate
            if let Some(binary) = self.binaries.pop_front() {
                match binary {
                    ConstantBinary::Scope { min, max } => {
                        let table_name = &self.table.name;
                        let index_meta = &self.index_meta;

                        let bound_encode = |bound: Bound<ValueRef>| -> Result<_, StorageError> {
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
                        let iter = self.tx.scan(encode_min, encode_max)?;
                        self.scope_iter = Some(iter);
                    }
                    ConstantBinary::Eq(val) => {
                        let key = self.val_to_key(val)?;
                        if let Some(bytes) = self.tx.get(&key)? {
                            if self.index_meta.is_unique {
                                for tuple_id in TableCodec::decode_index(&bytes)? {
                                    self.index_values.push_back(IndexValue::Normal(tuple_id));
                                }
                            } else if self.index_meta.is_primary {
                                let tuple =
                                    TableCodec::decode_tuple(self.table.all_columns(), &bytes);

                                self.index_values.push_back(IndexValue::PrimaryKey(tuple));
                            } else {
                                todo!()
                            }
                        }
                        self.scope_iter = None;
                    }
                    _ => (),
                }
            }
        }
        Ok(Some(tuples))
    }
}

pub struct MVCCTransaction<E: StorageEngine> {
    tx: mvcc::MVCCTransaction<E>,
}
impl<E: StorageEngine> Transaction for MVCCTransaction<E> {
    type IterType<'a> = MVCCIter<'a, E>;

    type IndexIterType<'a> = MVCCIndexIter<'a, E>;

    fn read(
        &self,
        table_name: TableName,
        bounds: Bounds,
        projection: Projections,
    ) -> Result<Self::IterType<'_>, StorageError> {
        let all_columns = self
            .table(table_name.clone())
            .ok_or(StorageError::TableNotFound)?
            .all_columns();
        let (min, max) = TableCodec::tuple_bound(&table_name);
        Ok(MVCCIter {
            offset: bounds.0.unwrap_or(0),
            limit: bounds.1,
            projection,
            all_columns,
            scan: self.tx.scan(Bound::Included(min), Bound::Included(max))?,
        })
    }

    fn read_by_index(
        &self,
        table_name: TableName,
        (offset_option, limit_option): Bounds,
        projection: Projections,
        index_meta: IndexMetaRef,
        binaries: Vec<ConstantBinary>,
    ) -> Result<Self::IndexIterType<'_>, StorageError> {
        let table = self
            .table(table_name.clone())
            .ok_or(StorageError::TableNotFound)?;
        let offset = offset_option.unwrap_or(0);
        Ok(MVCCIndexIter {
            offset,
            limit: limit_option,
            projection,
            index_meta,
            table,
            index_values: VecDeque::new(),
            binaries: VecDeque::from(binaries),
            tx: &self.tx,
            scope_iter: None,
        })
    }

    fn add_index(
        &mut self,
        table_name: &str,
        index: Index,
        tuple_ids: Vec<TupleId>,
        is_unique: bool,
    ) -> Result<(), StorageError> {
        let (key, value) = TableCodec::encode_index(table_name, &index, &tuple_ids)?;

        if let Some(bytes) = self.tx.get(&key)? {
            if is_unique {
                let old_tuple_ids = TableCodec::decode_index(&bytes)?;

                if old_tuple_ids[0] != tuple_ids[0] {
                    return Err(StorageError::DuplicateUniqueValue);
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

    fn del_index(&mut self, table_name: &str, index: &Index) -> Result<(), StorageError> {
        let key = TableCodec::encode_index_key(table_name, index)?;

        self.tx.delete(&key)?;

        Ok(())
    }

    fn append(
        &mut self,
        table_name: &str,
        tuple: Tuple,
        is_overwrite: bool,
    ) -> Result<(), StorageError> {
        let (key, value) = TableCodec::encode_tuple(table_name, &tuple)?;

        if !is_overwrite && self.tx.get(&key)?.is_some() {
            return Err(StorageError::DuplicatePrimaryKey);
        }
        self.tx.set(&key, value.to_vec())?;

        Ok(())
    }

    fn delete(&mut self, table_name: &str, tuple_id: TupleId) -> Result<(), StorageError> {
        let key = TableCodec::encode_tuple_key(table_name, &tuple_id)?;
        self.tx.delete(&key)?;

        Ok(())
    }

    fn add_column(
        &mut self,
        table_name: &TableName,
        column: &ColumnCatalog,
        if_not_exists: bool,
    ) -> Result<ColumnId, StorageError> {
        if let Some(mut catalog) = self.table(table_name.clone()) {
            if !column.nullable && column.default_value().is_none() {
                return Err(StorageError::NeedNullAbleOrDefault);
            }

            for col in catalog.all_columns() {
                if col.name() == column.name() {
                    if if_not_exists {
                        return Ok(col.id().unwrap());
                    } else {
                        return Err(StorageError::DuplicateColumn);
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

            Ok(col_id)
        } else {
            Err(StorageError::TableNotFound)
        }
    }

    fn drop_column(
        &mut self,
        table_name: &TableName,
        column: &str,
        if_exists: bool,
    ) -> Result<(), StorageError> {
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
                Err(MVCCError::KeyError(e)) => {
                    if !if_exists {
                        Err(MVCCError::KeyError(e))?;
                    }
                }
                err => err?,
            }

            Ok(())
        } else {
            Err(StorageError::TableNotFound)
        }
    }

    fn create_table(
        &mut self,
        table_name: TableName,
        columns: Vec<ColumnCatalog>,
        if_not_exists: bool,
    ) -> Result<TableName, StorageError> {
        let (table_key, value) = TableCodec::encode_root_table(&table_name)?;
        if self.tx.get(&table_key)?.is_some() {
            if if_not_exists {
                return Ok(table_name);
            }
            return Err(StorageError::TableExists);
        }
        self.tx.set(&table_key, value.to_vec())?;

        let mut table_catalog = TableCatalog::new(table_name.clone(), columns)?;

        Self::create_index_meta_for_table(&mut self.tx, &mut table_catalog)?;

        for column in table_catalog.columns.values() {
            let (key, value) = TableCodec::encode_column(&table_name, column)?;
            self.tx.set(&key, value.to_vec())?;
        }

        Ok(table_name)
    }

    fn drop_table(&mut self, table_name: &str, if_exists: bool) -> Result<(), StorageError> {
        if self.table(Arc::new(table_name.to_string())).is_none() {
            if if_exists {
                return Ok(());
            } else {
                return Err(StorageError::TableNotFound);
            }
        }
        self.drop_data(table_name)?;

        let (column_min, column_max) = TableCodec::columns_bound(table_name);
        Self::_drop_data(&mut self.tx, &column_min, &column_max)?;

        let (index_meta_min, index_meta_max) = TableCodec::index_meta_bound(table_name);
        Self::_drop_data(&mut self.tx, &index_meta_min, &index_meta_max)?;

        self.tx
            .delete(&TableCodec::encode_root_table_key(table_name))?;

        Ok(())
    }

    fn drop_data(&mut self, table_name: &str) -> Result<(), StorageError> {
        let (tuple_min, tuple_max) = TableCodec::tuple_bound(table_name);
        Self::_drop_data(&mut self.tx, &tuple_min, &tuple_max)?;

        let (index_min, index_max) = TableCodec::all_index_bound(table_name);
        Self::_drop_data(&mut self.tx, &index_min, &index_max)?;

        Ok(())
    }

    fn table(&self, table_name: TableName) -> Option<TableCatalog> {
        // TODO: unify the data into a `Meta` prefix and use one iteration to collect all data
        let columns = Self::column_collect(table_name.clone(), &self.tx).ok()?;
        let indexes = Self::index_meta_collect(&table_name, &self.tx)?;

        Some(
            TableCatalog::new_with_indexes(table_name.clone(), columns, indexes)
                .expect("fetch table error"),
        )
    }

    fn show_tables(&self) -> Result<Vec<String>, StorageError> {
        todo!()
    }

    fn commit(self) -> Result<(), StorageError> {
        self.tx.commit()?;

        Ok(())
    }

    fn rollback(self) -> Result<(), StorageError> {
        self.tx.rollback()?;

        Ok(())
    }
}

impl<E: StorageEngine> MVCCTransaction<E> {
    fn create_index_meta_for_table(
        tx: &mut mvcc::MVCCTransaction<E>,
        table: &mut TableCatalog,
    ) -> Result<(), StorageError> {
        let table_name = table.name.clone();

        for col in table
            .all_columns()
            .into_iter()
            .filter(|col| col.desc.is_index())
        {
            let is_primary = col.desc.is_primary;
            // FIXME: composite indexes may exist on future
            let prefix = if is_primary { "pk" } else { "uk" };

            if let Some(col_id) = col.id() {
                let meta_ref = table.add_index_meta(
                    format!("{}_{}", prefix, col.name()),
                    vec![col_id],
                    col.desc.is_unique,
                    is_primary,
                );
                let (key, value) = TableCodec::encode_index_meta(&table_name, meta_ref)?;

                tx.set(&key, value.to_vec())?;
            }
        }
        Ok(())
    }
    fn _drop_data(
        tx: &mut mvcc::MVCCTransaction<E>,
        min: &[u8],
        max: &[u8],
    ) -> Result<(), StorageError> {
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
    fn index_meta_collect(name: &str, tx: &mvcc::MVCCTransaction<E>) -> Option<Vec<IndexMetaRef>> {
        let (index_min, index_max) = TableCodec::index_meta_bound(name);
        let mut index_metas = vec![];
        let mut index_scan = tx
            .scan(Bound::Included(index_min), Bound::Included(index_max))
            .expect("scan index meta error");
        let mut index_iter = index_scan.iter();

        while let Ok(Some((_, value_option))) = index_iter.next().transpose() {
            if let Ok(index_meta) = TableCodec::decode_index_meta(&value_option) {
                index_metas.push(Arc::new(index_meta));
            }
        }

        Some(index_metas)
    }

    fn column_collect(
        table_name: TableName,
        tx: &mvcc::MVCCTransaction<E>,
    ) -> Result<Vec<ColumnCatalog>, StorageError> {
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
#[derive(Clone, Debug)]
pub struct MVCCLayer<E: StorageEngine> {
    layer: mvcc::MVCC<E>,
}
impl<E: StorageEngine> MVCCLayer<E> {
    pub fn new(engine: E) -> Self {
        Self {
            layer: MVCC::new(Arc::new(engine)),
        }
    }
}
impl<E: StorageEngine> Storage for MVCCLayer<E> {
    type TransactionType = MVCCTransaction<E>;

    fn transaction(&self) -> Result<Self::TransactionType, StorageError> {
        Ok(MVCCTransaction {
            tx: self.layer.begin(false)?,
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
    #[test]
    fn test_in_storage_works_with_data() -> Result<(), StorageError> {
        let storage = MVCCLayer::new(Memory::new());
        let mut transaction = storage.transaction()?;
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
            (None, None),
            vec![ScalarExpression::ColumnRef(columns[0].clone())],
        )?;

        let tuples = iter.fetch_tuple()?;
        println!("{:#?}",tuples);

        if let Some(tuples) = tuples {
            assert_eq!(tuples[0].id, Some(Arc::new(DataValue::Int32(Some(1)))));
            assert_eq!(tuples[0].values, vec![Arc::new(DataValue::Int32(Some(1))),],);
            assert_eq!(tuples[1].id, Some(Arc::new(DataValue::Int32(Some(2)))));
            assert_eq!(tuples[1].values, vec![Arc::new(DataValue::Int32(Some(2))),],);
        }
        Ok(())
    }
}
