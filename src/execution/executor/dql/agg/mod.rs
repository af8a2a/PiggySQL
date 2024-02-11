mod avg;
mod count;
pub mod hash_agg;
mod min_max;
pub mod simple_agg;
mod sum;
use crate::errors::*;

use crate::expression::agg::Aggregate;
use crate::expression::ScalarExpression;
use crate::types::value::ValueRef;

use self::{
    avg::AvgAccumulator,
    count::{CountAccumulator, DistinctCountAccumulator},
    min_max::MinMaxAccumulator,
    sum::{DistinctSumAccumulator, SumAccumulator},
};

/// Tips: Idea for sqlrs
/// An accumulator represents a stateful object that lives throughout the evaluation of multiple
/// rows and generically accumulates values.
pub trait Accumulator: Send + Sync {
    /// updates the accumulator's state from a vector of arrays.
    fn update_value(&mut self, value: &ValueRef) -> Result<()>;

    /// returns its value based on its current state.
    fn evaluate(&self) -> Result<ValueRef>;
}

fn create_accumulator(expr: &ScalarExpression) -> Box<dyn Accumulator> {
    if let ScalarExpression::AggCall {
        kind, ty, distinct, ..
    } = expr
    {
        match (kind, distinct) {
            (Aggregate::Count, false) => Box::new(CountAccumulator::new()),
            (Aggregate::Count, true) => Box::new(DistinctCountAccumulator::new()),
            (Aggregate::Sum, false) => Box::new(SumAccumulator::new(ty)),
            (Aggregate::Sum, true) => Box::new(DistinctSumAccumulator::new(ty)),
            (Aggregate::Min, _) => Box::new(MinMaxAccumulator::new(ty, false)),
            (Aggregate::Max, _) => Box::new(MinMaxAccumulator::new(ty, true)),
            (Aggregate::Avg, _) => Box::new(AvgAccumulator::new(ty)),
        }
    } else {
        unreachable!(
            "create_accumulator called with non-aggregate expression {:?}",
            expr
        );
    }
}

fn create_accumulators(exprs: &[ScalarExpression]) -> Vec<Box<dyn Accumulator>> {
    exprs.iter().map(create_accumulator).collect()
}
