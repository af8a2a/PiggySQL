use crate::binder::{lower_case_name, split_name, Binder};
use crate::errors::*;
use crate::planner::operator::drop_table::DropTableOperator;
use crate::planner::operator::Operator;
use crate::planner::LogicalPlan;
use crate::storage::Transaction;
use sqlparser::ast::ObjectName;
use std::sync::Arc;

impl<'a, T: Transaction> Binder<'a, T> {
    pub(crate) fn bind_drop_table(
        &mut self,
        name: &ObjectName,
        if_exists: &bool,
    ) -> Result<LogicalPlan> {
        // let name = lower_case_name(name);
        let (_, name) = split_name(&name)?;
        let table_name = Arc::new(name.to_string());

        let plan = LogicalPlan::new(
            Operator::DropTable(DropTableOperator {
                table_name,
                if_exists: *if_exists,
            }),
            vec![],
        );

        Ok(plan)
    }
}
