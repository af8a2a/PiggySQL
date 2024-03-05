use crate::binder::Binder;
use crate::planner::operator::set_var::SetVarOperator;
use crate::planner::operator::Operator;
use crate::planner::LogicalPlan;
use crate::storage::Transaction;

use crate::errors::*;

impl<'a, T: Transaction> Binder<'a, T> {
    pub(crate) fn bind_set_var(&mut self,var:String,val:String) -> Result<LogicalPlan> {
        let op=SetVarOperator{
            variable: var,
            value: val
        };
        let plan = LogicalPlan::new(Operator::SetVar(op), vec![]);
        Ok(plan)
    }
}
