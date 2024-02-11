use crate::execution::executor::{Source, Executor};

use crate::expression::ScalarExpression;
use crate::planner::operator::aggregate::AggregateOperator;
use crate::storage::Transaction;
use crate::types::tuple::Tuple;
use crate::types::value::ValueRef;

use itertools::Itertools;


use super::create_accumulators;

pub struct SimpleAggExecutor {
    pub agg_calls: Vec<ScalarExpression>,
    pub input: Source,
}

impl From<(AggregateOperator, Source)> for SimpleAggExecutor {
    fn from(
        (AggregateOperator { agg_calls, .. }, input): (AggregateOperator, Source),
    ) -> Self {
        SimpleAggExecutor { agg_calls, input }
    }
}

impl<T: Transaction> Executor<T> for SimpleAggExecutor {
    fn execute(self, _transaction: &mut T) -> Source {
        let mut accs = create_accumulators(&self.agg_calls);
        let mut columns_option = None;
        let mut tuples = Vec::new();
        for tuple in self.input?.iter() {

            columns_option.get_or_insert_with(|| {
                self.agg_calls
                    .iter()
                    .map(|expr| expr.output_columns())
                    .collect_vec()
            });

            let values: Vec<ValueRef> = self
                .agg_calls
                .iter()
                .map(|expr| match expr {
                    ScalarExpression::AggCall { args, .. } => args[0].eval(&tuple),
                    _ => unreachable!(),
                })
                .try_collect()?;

            for (acc, value) in accs.iter_mut().zip_eq(values.iter()) {
                acc.update_value(value)?;
            }
        }
        if let Some(columns) = columns_option {
            let values: Vec<ValueRef> = accs.into_iter().map(|acc| acc.evaluate()).try_collect()?;
            tuples.push(Tuple {
                id: None,
                columns,
                values,
            });
        }

        Ok(tuples)
    }
}
