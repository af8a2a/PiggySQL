use core::fmt;
use std::fmt::Formatter;

use crate::catalog::{IndexName, TableName};

#[derive(Debug, PartialEq, Clone)]
pub struct DropIndexOperator {
    pub table_name: TableName,
    pub index_name: IndexName,
    pub if_exists: bool,
}

impl fmt::Display for DropIndexOperator {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(
            f,
            "Drop Index {} of {} If Not Exists: {}",
            self.index_name, self.table_name, self.if_exists
        )?;
        Ok(())
    }
}
