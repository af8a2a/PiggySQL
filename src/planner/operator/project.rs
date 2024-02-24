use crate::catalog::ColumnRef;
use crate::expression::ScalarExpression;
use itertools::Itertools;
use std::fmt;
use std::fmt::Formatter;
#[derive(Debug, PartialEq, Clone)]
pub struct ProjectOperator {
    pub exprs: Vec<ScalarExpression>,
}

impl ProjectOperator {
    pub fn ouput_schema(&self) -> Vec<ColumnRef> {
        self.exprs
            .iter()
            .map(|expr| expr.output_columns())
            .collect()
    }
}

impl fmt::Display for ProjectOperator {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        let exprs = self.exprs.iter().map(|expr| format!("{}", expr)).join(", ");

        write!(f, "Projection [{}]", exprs)?;

        Ok(())
    }
}
