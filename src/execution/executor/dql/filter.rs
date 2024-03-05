use crate::execution::executor::{build, Executor, Source};

use crate::expression::ScalarExpression;
use crate::planner::operator::filter::FilterOperator;
use crate::planner::LogicalPlan;
use crate::storage::Transaction;

use crate::types::value::DataValue;

pub struct Filter {
    predicate: ScalarExpression,
    input: LogicalPlan,
}

impl From<(FilterOperator, LogicalPlan)> for Filter {
    fn from((FilterOperator { predicate, .. }, input): (FilterOperator, LogicalPlan)) -> Self {
        Filter { predicate, input }
    }
}

impl<T: Transaction> Executor<T> for Filter {
    fn execute(self, transaction: &mut T) -> Source {
        let Filter {
            predicate,
            mut input,
        } = self;
        let mut tuples = Vec::new();
        let schema = input.output_schema().clone();
        let input = build(input, transaction)?;
        for tuple in input {
            if let DataValue::Boolean(option) = predicate.eval(&tuple, &schema)?.as_ref() {
                if let Some(true) = option {
                    tuples.push(tuple.clone());
                } else {
                    continue;
                }
            } else {
                unreachable!("only bool");
            }
        }
        Ok(tuples)
    }
}
