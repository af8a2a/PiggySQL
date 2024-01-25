use std::sync::Arc;

use serde::{Deserialize, Serialize};

use crate::{expression::ScalarExpression, types::DataType};
#[derive(Debug, PartialEq, Eq, Clone, Serialize, Deserialize)]

pub struct Schema{
    pub table_name: String,
    pub columns: Vec<Column>,
    pub indexes: Vec<SchemaIndex>,
}

pub type ColumnRef = Arc<Column>;

#[derive(Debug, PartialEq,Hash, Eq, Clone, Serialize, Deserialize)]
pub struct Column{
    pub column_name: String,
    pub data_type: DataType,
    pub is_primary: bool,
    pub nullable: bool,
    pub is_unique: bool,

}
impl Column{
    pub fn new(column_name: String, data_type: DataType, is_primary: bool,nullable: bool, is_unique: bool) -> Self {
        Self {
            column_name,
            data_type,
            is_primary,
            nullable,
            is_unique,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct SchemaIndex {
    pub name: String,
    pub expr: ScalarExpression,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum IndexOperator {
    Gt,
    Lt,
    GtEq,
    LtEq,
    Eq,
}
