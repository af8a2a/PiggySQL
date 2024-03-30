pub mod engine;
pub mod piggy_stroage;
mod table_codec;

use crate::catalog::{ColumnCatalog, ColumnRef, IndexName, TableCatalog, TableName};

use crate::expression::simplify::ConstantBinary;
use crate::expression::ScalarExpression;
use crate::types::index::{Index, IndexMetaRef};
use crate::types::tuple::{Tuple, TupleId};
use crate::types::ColumnId;
use crate::{errors::*};

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
    fn read(
        &self,
        table_name: TableName,
        bound: Bounds,
        projection: Projections,
    ) -> Result<Self::IterType<'_>>;

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
    async fn commit(self) -> Result<()>;
    #[allow(async_fn_in_trait)]
    async fn rollback(self) -> Result<()>;

    fn set_isolation(&mut self, serializable: bool) -> Result<()>;
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
#[allow(dead_code)]
enum IndexValue {
    PrimaryKey(Tuple),
    Normal(TupleId),
}

pub trait Iter: Sync + Send {
    fn fetch_tuple(&mut self) -> Result<Option<Vec<Tuple>>>;
}

pub(crate) fn tuple_projection(
    projections: &Projections,
    schema: &[ColumnRef],
    tuple: Tuple,
) -> Result<Tuple> {
    let projection_len = projections.len();
    let mut values = Vec::with_capacity(projection_len);
    for expr in projections.iter() {
        values.push(expr.eval(&tuple, schema)?);
    }
    Ok(Tuple {
        id: tuple.id,
        values,
    })
}

