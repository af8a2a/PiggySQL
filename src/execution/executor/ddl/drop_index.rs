use crate::execution::executor::{Executor, Source};

use crate::planner::operator::drop_index::DropIndexOperator;
use crate::storage::Transaction;
use crate::types::tuple_builder::TupleBuilder;

pub struct DropIndex {
    op: DropIndexOperator,
}

impl From<DropIndexOperator> for DropIndex {
    fn from(op: DropIndexOperator) -> Self {
        DropIndex { op }
    }
}

impl<T: Transaction> Executor<T> for DropIndex {
    fn execute(self, transaction: &mut T) -> Source {
        let DropIndexOperator {
            index_name,
            table_name,
            if_exists,
        } = self.op;

        transaction.drop_index(table_name, index_name.clone(), if_exists)?;
        let tuple_builder = TupleBuilder::new_result();
        let tuple =
            tuple_builder.push_result("DROP INDEX SUCCESS", format!("{}", index_name).as_str())?;

        Ok(vec![tuple])
    }
}
