use std::collections::HashMap;

use serde::{Serialize, Deserialize};

use crate::types::{expression::Expression, DataType};


#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct Schema {
    pub table_name: String,
    pub column_defs: Option<Vec<Column>>,
    pub indexes: Vec<SchemaIndex>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct SchemaIndex {
    pub name: String,
    pub expr:Expression ,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct Column{
    pub name: String,
    pub data_type: DataType,
    pub nullable: bool,
    /// `DEFAULT <restricted-expr>`
    pub default: Option<Expression>,
    /// `{ PRIMARY KEY | UNIQUE }`
    pub unique: Option<bool>,

}