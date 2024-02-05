use crate::execution::executor::{BoxedExecutor, Executor};

use crate::expression::ScalarExpression;
use crate::planner::operator::filter::FilterOperator;
use crate::storage::Transaction;

use crate::types::value::DataValue;

use std::cell::RefCell;

pub struct Filter {
    predicate: ScalarExpression,
    input: BoxedExecutor,
}

impl From<(FilterOperator, BoxedExecutor)> for Filter {
    fn from((FilterOperator { predicate, .. }, input): (FilterOperator, BoxedExecutor)) -> Self {
        Filter { predicate, input }
    }
}

impl<T: Transaction> Executor<T> for Filter {
    fn execute(self, _transaction: &mut T) -> BoxedExecutor {
        let Filter { predicate, input } = self;
        let mut tuples= Vec::new();

        for tuple in input?.iter() {
            if let DataValue::Boolean(option) = predicate.eval(tuple)?.as_ref() {
                if let Some(true) = option {
                    tuples.push(tuple.clone());
                    // yield tuple;
                } else {
                    continue;
                }
            } else {
                unreachable!("only bool");
            }
        }
        Ok(tuples)
        // self._execute()
    }
}
