use crate::catalog::{IndexName, TableName};

#[derive(Debug, PartialEq, Clone)]
pub struct CreateIndexOperator {
    pub table_name: TableName,
    pub index_name: IndexName,
    pub col_name: String,
}
