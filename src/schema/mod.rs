use std::collections::HashMap;

use serde::{Serialize, Deserialize};
use sqlparser::ast::{ColumnDef, Expr};


#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct Schema {
    pub table_name: String,
    pub column_defs: Option<Vec<ColumnDef>>,
    pub indexes: Vec<SchemaIndex>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct SchemaIndex {
    pub name: String,
    pub expr: Expr,
}
