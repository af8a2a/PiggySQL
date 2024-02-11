use crate::binder::Binder;
use crate::planner::operator::Operator;
use crate::planner::LogicalPlan;
use crate::storage::Transaction;
use crate::errors::*;


impl<'a, T: Transaction> Binder<'a, T> {
    pub(crate) fn bind_explain(&mut self, plan: LogicalPlan) -> Result<LogicalPlan> {
        Ok(LogicalPlan {
            operator: Operator::Explain,
            childrens: vec![plan],
        })
    }
}
