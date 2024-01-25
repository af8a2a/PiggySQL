use crate::plan::TableName;

#[derive(Debug, PartialEq, Clone)]
pub struct DeleteOperator {
    pub table_name: TableName,
}
