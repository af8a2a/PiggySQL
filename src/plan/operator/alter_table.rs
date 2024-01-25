
use crate::store::schema::Column;

use crate::plan::TableName;

#[derive(Debug, PartialEq, Clone)]
pub struct AddColumnOperator {
    pub table_name: TableName,
    pub if_not_exists: bool,
    pub column: Column,
}

#[derive(Debug, PartialEq, Clone)]
pub struct DropColumnOperator {
    pub table_name: TableName,
    pub column_name: String,
    pub if_exists: bool,
}