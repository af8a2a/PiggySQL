use crate::{catalog::TableName, expression::ScalarExpression};

#[derive(Debug, PartialEq, Clone)]
pub struct UpdateOperator {
    pub set_expr:Vec<ScalarExpression>,
    pub table_name: TableName,
}
