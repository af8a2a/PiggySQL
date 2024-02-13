use crate::execution::executor::{Source, Executor};

use crate::planner::operator::limit::LimitOperator;
use crate::storage::{Transaction};






pub struct Limit {
    offset: Option<usize>,
    limit: Option<usize>,
    input: Source,
}

impl From<(LimitOperator, Source)> for Limit {
    fn from((LimitOperator { offset, limit }, input): (LimitOperator, Source)) -> Self {
        Limit {
            offset,
            limit,
            input,
        }
    }
}

impl<T: Transaction> Executor<T> for Limit {
    fn execute(self, _transaction: &mut T) -> Source {
        let mut tuples = Vec::new();
        let Limit {
            offset,
            limit,
            input,
        } = self;

        if limit.is_some() && limit.unwrap() == 0 {
            return Ok(tuples);
        }
        let input=input?;
        // println!("limit input tuple {}", input.len());

        let offset_val = offset.unwrap_or(0);
        let limit = limit.unwrap_or(usize::MAX);
        // println!("offset  {}", offset_val);
        // println!("limit  {}", limit);

        for (i, tuple) in input.iter().skip(offset_val).enumerate() {
            // if i < offset_val {
            //     continue;
            // }
              if i >= limit {
                break;
            }
            tuples.push(tuple.clone());
        }
        Ok(tuples)
    }
}
