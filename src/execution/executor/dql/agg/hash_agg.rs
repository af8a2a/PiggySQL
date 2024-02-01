use crate::execution::executor::{BoxedExecutor, Executor};

use crate::expression::ScalarExpression;
use crate::planner::operator::aggregate::AggregateOperator;
use crate::storage::Transaction;
use crate::types::tuple::Tuple;
use crate::types::value::ValueRef;
use ahash::{HashMap, HashMapExt};

use itertools::Itertools;
use std::cell::RefCell;

use super::create_accumulators;

pub struct HashAggExecutor {
    pub agg_calls: Vec<ScalarExpression>,
    pub groupby_exprs: Vec<ScalarExpression>,
    pub input: BoxedExecutor,
}

impl From<(AggregateOperator, BoxedExecutor)> for HashAggExecutor {
    fn from(
        (
            AggregateOperator {
                agg_calls,
                groupby_exprs,
            },
            input,
        ): (AggregateOperator, BoxedExecutor),
    ) -> Self {
        HashAggExecutor {
            agg_calls,
            groupby_exprs,
            input,
        }
    }
}

impl<T: Transaction> Executor<T> for HashAggExecutor {
    fn execute<'a>(self, _transaction: &RefCell<T>) -> BoxedExecutor {
        let mut group_and_agg_columns_option = None;
        let mut group_hash_accs = HashMap::new();
        let mut tuples = Vec::new();

        for tuple in self.input?.iter() {
            // 1. build group and agg columns for hash_agg columns.
            // Tips: AggCall First
            group_and_agg_columns_option.get_or_insert_with(|| {
                self.agg_calls
                    .iter()
                    .chain(self.groupby_exprs.iter())
                    .map(|expr| expr.output_columns())
                    .collect_vec()
            });

            // 2.1 evaluate agg exprs and collect the result values for later accumulators.
            let values: Vec<ValueRef> = self
                .agg_calls
                .iter()
                .map(|expr| {
                    if let ScalarExpression::AggCall { args, .. } = expr {
                        args[0].eval(&tuple)
                    } else {
                        unreachable!()
                    }
                })
                .try_collect()?;

            let group_keys: Vec<ValueRef> = self
                .groupby_exprs
                .iter()
                .map(|expr| expr.eval(&tuple))
                .try_collect()?;

            for (acc, value) in group_hash_accs
                .entry(group_keys)
                .or_insert_with(|| create_accumulators(&self.agg_calls))
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
                tuples.push(Tuple {
                    id: None,
                    columns: group_and_agg_columns.clone(),
                    values,
                });
            }
        }
        Ok(tuples)
    }
}

// impl HashAggExecutor {
//     #[try_stream(boxed, ok = Tuple, error = ExecutorError)]
//     pub async fn _execute(self) {
//         let mut group_and_agg_columns_option = None;
//         let mut group_hash_accs = HashMap::new();

//         #[for_await]
//         for tuple in self.input {
//             let tuple = tuple?;

//             // 1. build group and agg columns for hash_agg columns.
//             // Tips: AggCall First
//             group_and_agg_columns_option.get_or_insert_with(|| {
//                 self.agg_calls
//                     .iter()
//                     .chain(self.groupby_exprs.iter())
//                     .map(|expr| expr.output_columns())
//                     .collect_vec()
//             });

//             // 2.1 evaluate agg exprs and collect the result values for later accumulators.
//             let values: Vec<ValueRef> = self
//                 .agg_calls
//                 .iter()
//                 .map(|expr| {
//                     if let ScalarExpression::AggCall { args, .. } = expr {
//                         args[0].eval(&tuple)
//                     } else {
//                         unreachable!()
//                     }
//                 })
//                 .try_collect()?;

//             let group_keys: Vec<ValueRef> = self
//                 .groupby_exprs
//                 .iter()
//                 .map(|expr| expr.eval(&tuple))
//                 .try_collect()?;

//             for (acc, value) in group_hash_accs
//                 .entry(group_keys)
//                 .or_insert_with(|| create_accumulators(&self.agg_calls))
//                 .iter_mut()
//                 .zip_eq(values.iter())
//             {
//                 acc.update_value(value)?;
//             }
//         }

//         if let Some(group_and_agg_columns) = group_and_agg_columns_option {
//             for (group_keys, accs) in group_hash_accs {
//                 // Tips: Accumulator First
//                 let values: Vec<ValueRef> = accs
//                     .iter()
//                     .map(|acc| acc.evaluate())
//                     .chain(group_keys.into_iter().map(Ok))
//                     .try_collect()?;

//                 yield Tuple {
//                     id: None,
//                     columns: group_and_agg_columns.clone(),
//                     values,
//                 };
//             }
//         }
//     }
// }
