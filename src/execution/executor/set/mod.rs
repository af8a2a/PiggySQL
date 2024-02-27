use crate::{planner::operator::set_var::SetVarOperator, storage::Transaction};

use super::{Executor, Source};

pub struct SetVariable {
    op: SetVarOperator,
}

impl From<SetVarOperator> for SetVariable {
    fn from(op: SetVarOperator) -> Self {
        SetVariable { op }
    }
}

impl<T: Transaction> Executor<T> for SetVariable {
    fn execute(self, transaction: &mut T) -> Source {
        if self.op.value == "serializable" {
            transaction.set_isolation(true)?;
        } else {
            transaction.set_isolation(false)?;
        }

        Ok(vec![])
    }
}
