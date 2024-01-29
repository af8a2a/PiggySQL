mod engine;
mod mvcc;
mod table_codec;
mod keycode;
use crate::catalog::{CatalogError, ColumnCatalog, TableCatalog, TableName};
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

pub trait Storage: Sync + Send + Clone + 'static {
    type TransactionType: Transaction;

    #[allow(async_fn_in_trait)]
    async fn transaction(&self) -> Result<Self::TransactionType, StorageError>;
}


/// Optional bounds of the reader, of the form (offset, limit).
pub(crate) type Bounds = (Option<usize>, Option<usize>);
type Projections = Vec<ScalarExpression>;

pub trait Transaction: Sync + Send + 'static {
    type IterType<'a>: Iter;

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
    ) -> Result<Self::IterType<'_>, StorageError>;

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
}


pub struct TransactionImpl{

}

