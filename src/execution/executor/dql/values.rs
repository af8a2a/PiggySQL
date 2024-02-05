use crate::execution::executor::{BoxedExecutor, Executor};

use crate::planner::operator::values::ValuesOperator;
use crate::storage::Transaction;
use crate::types::tuple::Tuple;

use itertools::Itertools;
use std::cell::RefCell;

pub struct Values {
    op: ValuesOperator,
}

impl From<ValuesOperator> for Values {
    fn from(op: ValuesOperator) -> Self {
        Values { op }
    }
}

impl<T: Transaction> Executor<T> for Values {
    fn execute(self, _transaction: &mut T) -> BoxedExecutor {
        let ValuesOperator { columns, rows } = self.op;
        // eprintln!("values executor result :{:#?}",columns);
        
        Ok(rows
            .iter()
            .map(|val| Tuple {
                id: None,
                columns: columns.clone(),
                values: val.clone(),
            })
            .collect_vec())
    }
}
