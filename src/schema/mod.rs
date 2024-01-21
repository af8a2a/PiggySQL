use std::collections::HashMap;

use sqlparser::ast::ColumnDef;

pub struct Catalog {
    tables: HashMap<String, Schema>,
}

pub struct Schema {
    name: String,
    cols: Vec<ColumnDef>,
}
