use std::fmt::{self, Formatter};

use crate::catalog::{IndexName, TableName};

#[derive(Debug, PartialEq, Clone)]
pub struct CreateIndexOperator {
    pub table_name: TableName,
    pub index_name: IndexName,
    pub col_name: String,
}

impl fmt::Display for CreateIndexOperator {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(
            f,
            "Create Index {} on {}.{}",
            self.index_name, self.table_name, self.col_name
        )?;
        Ok(())
    }
}
