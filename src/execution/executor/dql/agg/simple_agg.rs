use crate::execution::executor::{build, Executor, Source};

use crate::expression::ScalarExpression;
use crate::planner::operator::aggregate::AggregateOperator;
use crate::planner::LogicalPlan;
use crate::storage::Transaction;
use crate::types::tuple::Tuple;
use crate::types::value::ValueRef;

use itertools::Itertools;

use super::create_accumulators;

pub struct SimpleAggExecutor {
    pub agg_calls: Vec<ScalarExpression>,
    pub input: LogicalPlan,
}

impl From<(AggregateOperator, LogicalPlan)> for SimpleAggExecutor {
    fn from(
        (AggregateOperator { agg_calls, .. }, input): (AggregateOperator, LogicalPlan),
    ) -> Self {
        SimpleAggExecutor { agg_calls, input }
    }
}

impl<T: Transaction> Executor<T> for SimpleAggExecutor {
    fn execute(self, transaction: &mut T) -> Source {
        let SimpleAggExecutor {
            agg_calls,
            mut input,
        } = self;

        let mut accs = create_accumulators(&agg_calls);
        let mut columns_option = None;
        let mut tuples = Vec::new();
        let schema = input.output_schema().clone();
        let input = build(input, transaction)?;
        for tuple in input {
            columns_option.get_or_insert_with(|| {
                agg_calls
                    .iter()
                    .map(|expr| expr.output_columns())
                    .collect_vec()
            });

            let values: Vec<ValueRef> = agg_calls
                .iter()
                .map(|expr| match expr {
                    ScalarExpression::AggCall { args, .. } => args[0].eval(&tuple, &schema),
                    _ => unreachable!(),
                })
                .try_collect()?;

            for (acc, value) in accs.iter_mut().zip_eq(values.iter()) {
                acc.update_value(value)?;
            }
        }
        if let Some(columns) = columns_option {
            let values: Vec<ValueRef> = accs.into_iter().map(|acc| acc.evaluate()).try_collect()?;
            tuples.push(Tuple { id: None, values });
        }

        Ok(tuples)
    }
}
