use crate::catalog::TableName;
use std::fmt;
use std::fmt::Formatter;

#[derive(Debug, PartialEq, Clone)]
pub struct DropTableOperator {
    /// Table name to insert to
    pub table_name: TableName,
    pub if_exists: bool,
}
impl fmt::Display for DropTableOperator {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "Drop {}, If Exists: {}", self.table_name, self.if_exists)?;

        Ok(())
    }
}
