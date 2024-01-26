use crate::binder::{lower_case_name, split_name, BindError, Binder};
use crate::plan::operator::drop_table::DropTableOperator;
use crate::plan::operator::Operator;
use crate::plan::LogicalPlan;
use crate::storage::Transaction;
use sqlparser::ast::ObjectName;
use std::sync::Arc;

impl<'a, T: Transaction> Binder<'a, T> {
    pub(crate) fn bind_drop_table(
        &mut self,
        name: &ObjectName,
        if_exists: &bool,
    ) -> Result<LogicalPlan, BindError> {
        let name = lower_case_name(name);
        let (_, name) = split_name(&name)?;
        let table_name = Arc::new(name.to_string());

        let plan = LogicalPlan {
            operator: Operator::DropTable(DropTableOperator {
                table_name,
                if_exists: *if_exists,
            }),
            childrens: vec![],
        };
        Ok(plan)
    }
}
