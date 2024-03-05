use crate::execution::executor::{build, Executor, Source};

use crate::planner::operator::limit::LimitOperator;
use crate::planner::LogicalPlan;
use crate::storage::Transaction;

pub struct Limit {
    offset: Option<usize>,
    limit: Option<usize>,
    input: LogicalPlan,
}

impl From<(LimitOperator, LogicalPlan)> for Limit {
    fn from((LimitOperator { offset, limit }, input): (LimitOperator, LogicalPlan)) -> Self {
        Limit {
            offset,
            limit,
            input,
        }
    }
}

impl<T: Transaction> Executor<T> for Limit {
    fn execute(self, transaction: &mut T) -> Source {
        let mut tuples = Vec::new();
        let Limit {
            offset,
            limit,
            mut input,
        } = self;
        let input = build(input, transaction)?;
        if limit.is_some() && limit.unwrap_or(0) == 0 {
            return Ok(tuples);
        }
        // println!("limit input tuple {}", input.len());

        let offset_val = offset.unwrap_or(0);
        let limit = limit.unwrap_or(usize::MAX);
        // debug!("offset  {}", offset_val);
        // debug!("limit  {}", limit);

        for (i, tuple) in input.iter().skip(offset_val).enumerate() {
            if i >= limit {
                break;
            }
            tuples.push(tuple.clone());
        }
        Ok(tuples)
    }
}
