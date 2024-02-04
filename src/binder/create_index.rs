use std::sync::Arc;

use sqlparser::ast::{ObjectName, OrderByExpr};

use crate::{binder::{lower_case_name, split_name}, planner::LogicalPlan, storage::Transaction};

use super::{BindError, Binder};

impl<'a, T: Transaction> Binder<'a, T> {
    pub(crate) fn bind_create_index(
        &mut self,
        name: &ObjectName,
        table_name: &ObjectName,
        columns: Vec<OrderByExpr>,
    ) -> Result<LogicalPlan, BindError> {
        let name = lower_case_name(name);
        let (_, name) = split_name(&name)?;
        let table_name = Arc::new(name.to_string());
        
        todo!()
    }
}
