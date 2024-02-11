use crate::binder::Binder;
use crate::planner::operator::Operator;
use crate::planner::LogicalPlan;
use crate::storage::Transaction;

use crate::errors::*;

impl<'a, T: Transaction> Binder<'a, T> {
    pub(crate) fn bind_show_tables(&mut self) -> Result<LogicalPlan> {
        let plan = LogicalPlan {
            operator: Operator::Show,
            childrens: vec![],
        };
        Ok(plan)
    }
}
