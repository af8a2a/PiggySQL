
use crate::store::schema::Column;

use crate::plan::TableName;

#[derive(Debug, PartialEq, Clone)]
pub struct CreateTableOperator {
    /// Table name to insert to
    pub table_name: TableName,
    /// List of columns of the table
    pub columns: Vec<Column>,
    pub if_not_exists: bool,
}
