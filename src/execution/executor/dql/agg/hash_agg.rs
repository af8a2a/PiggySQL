use crate::execution::executor::{build, Executor, Source};

use crate::expression::ScalarExpression;
use crate::planner::operator::aggregate::AggregateOperator;
use crate::planner::LogicalPlan;
use crate::storage::Transaction;
use crate::types::tuple::Tuple;
use crate::types::value::ValueRef;
use ahash::{HashMap, HashMapExt};

use itertools::Itertools;

use super::create_accumulators;

pub struct HashAggExecutor {
    pub agg_calls: Vec<ScalarExpression>,
    pub groupby_exprs: Vec<ScalarExpression>,
    pub input: LogicalPlan,
}

impl From<(AggregateOperator, LogicalPlan)> for HashAggExecutor {
    fn from(
        (
            AggregateOperator {
                agg_calls,
                groupby_exprs,
                ..
            },
            input,
        ): (AggregateOperator, LogicalPlan),
    ) -> Self {
        HashAggExecutor {
            agg_calls,
            groupby_exprs,
            input,
        }
    }
}

impl<T: Transaction> Executor<T> for HashAggExecutor {
    fn execute<'a>(self, transaction: &mut T) -> Source {
        let HashAggExecutor {
            agg_calls,
            groupby_exprs,
            mut input,
        } = self;
        let schema = input.output_schema().clone();
        let input = build(input, transaction)?;
        let mut group_and_agg_columns_option = None;
        let mut group_hash_accs = HashMap::new();
        let mut tuples = Vec::new();

        for tuple in input {
            // 1. build group and agg columns for hash_agg columns.
            // Tips: AggCall First
            group_and_agg_columns_option.get_or_insert_with(|| {
                agg_calls
                    .iter()
                    .chain(groupby_exprs.iter())
                    .map(|expr| expr.output_columns())
                    .collect_vec()
            });

            // 2.1 evaluate agg exprs and collect the result values for later accumulators.
            let values: Vec<ValueRef> = agg_calls
                .iter()
                .map(|expr| {
                    if let ScalarExpression::AggCall { args, .. } = expr {
                        args[0].eval(&tuple, &schema)
                    } else {
                        unreachable!()
                    }
                })
                .try_collect()?;

            let group_keys: Vec<ValueRef> = groupby_exprs
                .iter()
                .map(|expr| expr.eval(&tuple, &schema))
                .try_collect()?;

            for (acc, value) in group_hash_accs
                .entry(group_keys)
                .or_insert_with(|| create_accumulators(&agg_calls))
                .iter_mut()
                .zip_eq(values.iter())
            {
                acc.update_value(value)?;
            }
        }
        if let Some(group_and_agg_columns) = group_and_agg_columns_option {
            for (group_keys, accs) in group_hash_accs {
                // Tips: Accumulator First
                let values: Vec<ValueRef> = accs
                    .iter()
                    .map(|acc| acc.evaluate())
                    .chain(group_keys.into_iter().map(Ok))
                    .try_collect()?;
                tuples.push(Tuple { id: None, values });
            }
        }
        Ok(tuples)
    }
}
