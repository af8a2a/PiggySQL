use std::sync::Arc;

use crate::{
    catalog::ColumnCatalog,
    execution::executor::{Source, Executor},
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
        let columns = vec![Arc::new(ColumnCatalog::new_dummy("PLAN".to_string()))];
        let values = vec![Arc::new(DataValue::Utf8(Some(self.plan.explain(0))))];
        Ok(vec![Tuple {
            id: None,
            values,
        }])
    }
}
