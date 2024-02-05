use crate::catalog::TableName;

use std::fmt;
use std::fmt::Formatter;

#[derive(Debug, PartialEq, Clone)]
pub struct DeleteOperator {
    pub table_name: TableName,
}
impl fmt::Display for DeleteOperator {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "Delete {}", self.table_name)?;

        Ok(())
    }
}