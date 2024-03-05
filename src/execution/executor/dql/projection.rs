use crate::execution::executor::{build, Executor, Source};

use crate::expression::ScalarExpression;
use crate::planner::operator::project::ProjectOperator;
use crate::planner::LogicalPlan;
use crate::storage::Transaction;

pub struct Projection {
    pub(crate) exprs: Vec<ScalarExpression>,
    input: LogicalPlan,
}

impl From<(ProjectOperator, LogicalPlan)> for Projection {
    fn from((ProjectOperator { exprs }, input): (ProjectOperator, LogicalPlan)) -> Self {
        Projection { exprs, input }
    }
}

impl<T: Transaction> Executor<T> for Projection {
    fn execute(self, transaction: &mut T) -> Source {
        let Projection { exprs, mut input } = self;
        let mut tuples = Vec::new();
        let schema = input.output_schema().clone();
        let mut data_source = build(input, transaction)?;
        for tuple in data_source.iter_mut() {
            // let tuple = tuple;

            let mut values = Vec::with_capacity(exprs.len());

            for expr in exprs.iter() {
                values.push(expr.eval(&tuple, &schema)?);
                // columns.push(expr.output_columns());
            }
            // tuple.columns = columns;
            tuple.values = values;

            tuples.push(tuple.clone());
        }
        for tuple in &tuples {
            println!("proj:{}", tuple);
        }
        Ok(tuples)
    }
}
