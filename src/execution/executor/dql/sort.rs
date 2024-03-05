use crate::execution::executor::{build, Executor, Source};

use crate::planner::operator::sort::{SortField, SortOperator};
use crate::planner::LogicalPlan;
use crate::storage::Transaction;
use crate::types::tuple::Tuple;

use std::cmp::Ordering;

pub struct Sort {
    sort_fields: Vec<SortField>,
    limit: Option<usize>,
    input: LogicalPlan,
}

impl From<(SortOperator, LogicalPlan)> for Sort {
    fn from((SortOperator { sort_fields, limit }, input): (SortOperator, LogicalPlan)) -> Self {
        Sort {
            sort_fields,
            limit,
            input,
        }
    }
}

impl<T: Transaction> Executor<T> for Sort {
    fn execute(self, transaction: &mut T) -> Source {
        let Sort {
            sort_fields,
            limit,
            mut input,
        } = self;
        let mut tuples: Vec<Tuple> = vec![];
        let schema = input.output_schema().clone();
        let input = build(input, transaction)?;
        for tuple in input {
            tuples.push(tuple);
        }
        tuples.sort_by(|tuple_1, tuple_2| {
            let mut ordering = Ordering::Equal;

            for SortField {
                expr,
                asc,
                nulls_first,
            } in &sort_fields
            {
                let value_1 = expr.eval(tuple_1, &schema).unwrap();
                let value_2 = expr.eval(tuple_2, &schema).unwrap();

                ordering = value_1.partial_cmp(&value_2).unwrap_or_else(|| {
                    match (value_1.is_null(), value_2.is_null()) {
                        (false, true) => {
                            if *nulls_first {
                                Ordering::Less
                            } else {
                                Ordering::Greater
                            }
                        }
                        (true, false) => {
                            if *nulls_first {
                                Ordering::Greater
                            } else {
                                Ordering::Less
                            }
                        }
                        _ => Ordering::Equal,
                    }
                });

                if !*asc {
                    ordering = ordering.reverse();
                }

                if ordering != Ordering::Equal {
                    break;
                }
            }

            ordering
        });
        let len = limit.unwrap_or(tuples.len());
        tuples = tuples.drain(..len).collect();

        Ok(tuples)
    }
}
