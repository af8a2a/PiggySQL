use crate::execution::executor::{BoxedExecutor, Executor};

use crate::planner::operator::drop_table::DropTableOperator;
use crate::storage::Transaction;

use std::cell::RefCell;

pub struct DropTable {
    op: DropTableOperator,
}

impl From<DropTableOperator> for DropTable {
    fn from(op: DropTableOperator) -> Self {
        DropTable { op }
    }
}

impl<T: Transaction> Executor<T> for DropTable {
    fn execute(self, transaction: &mut T) -> BoxedExecutor {
        let DropTableOperator {
            table_name,
            if_exists,
        } = self.op;

        transaction.drop_table(&table_name, if_exists)?;
        Ok(vec![])
    }
}
