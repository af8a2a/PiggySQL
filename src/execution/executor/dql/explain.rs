use std::sync::Arc;

use crate::{
    execution::executor::{Executor, Source},
    planner::LogicalPlan,
    storage::Transaction,
    types::{tuple::Tuple, value::DataValue},
};

pub struct Explain {
    plan: LogicalPlan,
}
impl From<LogicalPlan> for Explain {
    fn from(plan: LogicalPlan) -> Self {
        Explain { plan }
    }
}
impl<T: Transaction> Executor<T> for Explain {
    fn execute(self, _: &mut T) -> Source {
        let values = vec![Arc::new(DataValue::Utf8(Some(self.plan.explain(0))))];
        Ok(vec![Tuple { id: None, values }])
    }
}
