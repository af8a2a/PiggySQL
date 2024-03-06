use std::fmt;
use std::fmt::Formatter;
use std::sync::Arc;

use crate::{
    catalog::{ColumnCatalog, TableName},
    expression::ScalarExpression,
};

#[derive(Debug, PartialEq, Clone)]
pub struct UpdateOperator {
    pub columns: Vec<Arc<ColumnCatalog>>,
    pub set_expr: Vec<ScalarExpression>,
    pub table_name: TableName,
}
impl fmt::Display for UpdateOperator {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "Update {}", self.table_name)?;

        Ok(())
    }
}
