use crate::execution::executor::{Executor, Source};

use crate::expression::ScalarExpression;
use crate::planner::operator::project::ProjectOperator;
use crate::storage::Transaction;

pub struct Projection {
    pub(crate) exprs: Vec<ScalarExpression>,
    input: Source,
}

impl From<(ProjectOperator, Source)> for Projection {
    fn from((ProjectOperator { exprs }, input): (ProjectOperator, Source)) -> Self {
        Projection { exprs, input }
    }
}

impl<T: Transaction> Executor<T> for Projection {
    fn execute(self, _transaction: &mut T) -> Source {
        let Projection { exprs, input } = self;
        let mut tuples = Vec::new();
        for tuple in input?.iter_mut() {
            // let tuple = tuple;

            let mut columns = Vec::with_capacity(exprs.len());
            let mut values = Vec::with_capacity(exprs.len());

            for expr in exprs.iter() {
                values.push(expr.eval(&tuple)?);
                columns.push(expr.output_columns());
            }
            tuple.columns = columns;
            tuple.values = values;

            tuples.push(tuple.clone());
        }
        Ok(tuples)
    }
}
