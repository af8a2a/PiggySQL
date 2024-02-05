use std::fmt::{self, Formatter};

use crate::catalog::{ColumnCatalog, TableName};

#[derive(Debug, PartialEq, Clone)]
pub struct AddColumnOperator {
    pub table_name: TableName,
    pub if_not_exists: bool,
    pub column: ColumnCatalog,
}
impl fmt::Display for AddColumnOperator {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(
            f,
            "Add {} -> {}, If Not Exists: {}",
            self.column.name(),
            self.table_name,
            self.if_not_exists
        )?;

        Ok(())
    }
}

#[derive(Debug, PartialEq, Clone)]
pub struct DropColumnOperator {
    pub table_name: TableName,
    pub column_name: String,
    pub if_exists: bool,
}

impl fmt::Display for DropColumnOperator {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(
            f,
            "Drop {} -> {}, If Exists: {}",
            self.column_name, self.table_name, self.if_exists
        )?;

        Ok(())
    }
}
