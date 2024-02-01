mod engine;
mod keycode;
pub(crate) mod kip_impl;
pub mod mvcc;
mod table_codec;
use kip_db::KernelError;

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

use self::engine::StorageEngine;
use self::mvcc::{MVCCError, Scan};

pub trait Storage: Sync + Send + Clone {
    type TransactionType: Transaction;

    #[allow(async_fn_in_trait)]
    async fn transaction(&self) -> Result<Self::TransactionType, StorageError>;
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
    fn table(&self, table_name: TableName) -> Option<&TableCatalog>;

    fn show_tables(&self) -> Result<Vec<String>, StorageError>;

    #[allow(async_fn_in_trait)]
    async fn commit(self) -> Result<(), StorageError>;

    #[allow(async_fn_in_trait)]
    async fn rollback(self) -> Result<(), StorageError>;
}

enum IndexValue {
    PrimaryKey(Tuple),
    Normal(TupleId),
}

pub trait Iter: Sync + Send {
    fn next_tuple(&mut self) -> Result<Option<Tuple>, StorageError>;
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

    #[error("kipdb error")]
    KipDBError(KernelError),

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
    fn next_tuple(&mut self) -> Result<Option<Tuple>, StorageError> {
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
        while let Some(item) = iter.next() {
            if let Ok((_, value)) = item.map_err(|e| StorageError::from(e)) {
                let tuple = tuple_projection(
                    &mut self.limit,
                    &self.projection,
                    TableCodec::decode_tuple(self.all_columns.clone(), &value),
                )?;

                return Ok(Some(tuple));
            }
        }

        Ok(None)
    }
}
pub struct MVCCIndexIter<'a, E: StorageEngine> {
    offset: usize,
    limit: Option<usize>,
    projections: Projections,

    index_meta: IndexMetaRef,
    table: &'a TableCatalog,
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

                tuple_projection(&mut self.limit, &self.projections, tuple)
            })
            .transpose()
    }

    fn is_empty(&self) -> bool {
        self.scope_iter.is_none() && self.index_values.is_empty() && self.binaries.is_empty()
    }
}

impl<E: StorageEngine> Iter for MVCCIndexIter<'_, E> {
    fn next_tuple(&mut self) -> Result<Option<Tuple>, StorageError> {
        if matches!(self.limit, Some(0)) || self.is_empty() {
            self.scope_iter = None;
            self.binaries.clear();

            return Ok(None);
        }
        // 2. try get tuple on index_values and until it empty
        loop {
            if let Some(value) = self.index_values.pop_front() {
                if Self::offset_move(&mut self.offset) {
                    continue;
                }
                match value {
                    IndexValue::PrimaryKey(tuple) => {
                        let tuple = tuple_projection(&mut self.limit, &self.projections, tuple)?;

                        return Ok(Some(tuple));
                    }
                    IndexValue::Normal(tuple_id) => {
                        if let Some(tuple) = self.get_tuple_by_id(&tuple_id)? {
                            return Ok(Some(tuple));
                        }
                    }
                }
            } else {
                break;
            }
        }
        assert!(self.index_values.is_empty());

        todo!()
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
            scan: self.tx.scan(min..=max)?,
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
        Ok(IndexIter {
            offset,
            limit: limit_option,
            projections,
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
        todo!()
    }

    fn del_index(&mut self, table_name: &str, index: &Index) -> Result<(), StorageError> {
        todo!()
    }

    fn append(
        &mut self,
        table_name: &str,
        tuple: Tuple,
        is_overwrite: bool,
    ) -> Result<(), StorageError> {
        todo!()
    }

    fn delete(&mut self, table_name: &str, tuple_id: TupleId) -> Result<(), StorageError> {
        todo!()
    }

    fn add_column(
        &mut self,
        table_name: &TableName,
        column: &ColumnCatalog,
        if_not_exists: bool,
    ) -> Result<ColumnId, StorageError> {
        todo!()
    }

    fn drop_column(
        &mut self,
        table_name: &TableName,
        column: &str,
        if_exists: bool,
    ) -> Result<(), StorageError> {
        todo!()
    }

    fn create_table(
        &mut self,
        table_name: TableName,
        columns: Vec<ColumnCatalog>,
        if_not_exists: bool,
    ) -> Result<TableName, StorageError> {
        todo!()
    }

    fn drop_table(&mut self, table_name: &str, if_exists: bool) -> Result<(), StorageError> {
        todo!()
    }

    fn drop_data(&mut self, table_name: &str) -> Result<(), StorageError> {
        todo!()
    }

    fn table(&self, table_name: TableName) -> Option<&TableCatalog> {
        todo!()
    }

    fn show_tables(&self) -> Result<Vec<String>, StorageError> {
        todo!()
    }

    async fn commit(self) -> Result<(), StorageError> {
        todo!()
    }

    async fn rollback(self) -> Result<(), StorageError> {
        todo!()
    }
}
