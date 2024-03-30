

use crate::catalog::ColumnRef;
use crate::errors::*;
use crate::execution::executor::{build, Executor, Source};
use crate::expression::ScalarExpression;
use crate::planner::operator::project::ProjectOperator;
use crate::planner::LogicalPlan;
use crate::storage::Transaction;
use crate::types::tuple::Tuple;
use crate::types::value::ValueRef;

pub struct Projection {
    pub(crate) exprs: Vec<ScalarExpression>,
    input: LogicalPlan,
}

impl From<(ProjectOperator, LogicalPlan)> for Projection {
    fn from((ProjectOperator { exprs }, input): (ProjectOperator, LogicalPlan)) -> Self {
        Projection { exprs, input }
    }
}
impl Projection {
    pub fn projection(
        tuple: &Tuple,
        exprs: &[ScalarExpression],
        schmea: &[ColumnRef],
    ) -> Result<Vec<ValueRef>> {
        let mut values = Vec::with_capacity(exprs.len());
        // println!("exprs:{}",exprs.iter().map(|expr|expr.to_string()).join(","));
        for expr in exprs.iter() {
            values.push(expr.eval(tuple, schmea)?);
        }
        Ok(values)
    }
}

impl<T: Transaction> Executor<T> for Projection {
    fn execute(self, transaction: &mut T) -> Source {
        let Projection { exprs, mut input } = self;
        let mut tuples = Vec::new();
        let schema = input.output_schema().clone();
        let mut data_source = build(input, transaction)?;
        for tuple in data_source.iter_mut() {
            // println!("projection before: {}", tuple);
            // let tuple = tuple;
            let values = Self::projection(tuple, &exprs, &schema)?;
            // println!("projection after: {}",values.iter().map(|v| v.to_string()).join(","));

            tuple.values = values;
            tuples.push(tuple.clone());
        }
        Ok(tuples)
    }
}
