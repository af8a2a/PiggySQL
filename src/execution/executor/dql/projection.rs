use crate::execution::executor::{BoxedExecutor, Executor};

use crate::expression::ScalarExpression;
use crate::planner::operator::project::ProjectOperator;
use crate::storage::Transaction;
use crate::types::tuple::{Tuple};

use std::cell::RefCell;

pub struct Projection {
    exprs: Vec<ScalarExpression>,
    input: BoxedExecutor,
}

impl From<(ProjectOperator, BoxedExecutor)> for Projection {
    fn from((ProjectOperator { exprs }, input): (ProjectOperator, BoxedExecutor)) -> Self {
        Projection { exprs, input }
    }
}

impl<T: Transaction> Executor<T> for Projection {
    fn execute(self, _transaction: &RefCell<T>) -> BoxedExecutor {
        let Projection { exprs, input } = self;
        let mut tuples = Vec::new();
        for tuple in input?.iter() {
            let tuple = tuple;

            let mut columns = Vec::with_capacity(exprs.len());
            let mut values = Vec::with_capacity(exprs.len());

            for expr in exprs.iter() {
                values.push(expr.eval(&tuple)?);
                columns.push(expr.output_columns());
            }
            tuples.push(Tuple {
                id: None,
                columns,
                values,
            });
        }
        Ok(tuples)
    }
}
