use std::sync::Arc;

use serde::{Deserialize, Serialize};

use crate::{expression::ScalarExpression, types::DataType};

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
