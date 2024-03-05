use crate::binder::Binder;
use crate::errors::DatabaseError;
use crate::planner::operator::describe::DescribeOperator;
use crate::planner::operator::Operator;
use crate::planner::LogicalPlan;
use crate::storage::Transaction;
use sqlparser::ast::ObjectName;
use std::sync::Arc;

impl<'a, T: Transaction> Binder<'a, T> {
    pub(crate) fn bind_describe(
        &mut self,
        name: &ObjectName,
    ) -> Result<LogicalPlan, DatabaseError> {
        let table_name = Arc::new(name.to_string());

        Ok(LogicalPlan {
            operator: Operator::Describe(DescribeOperator { table_name }),
            childrens: vec![],
        })
    }
}
