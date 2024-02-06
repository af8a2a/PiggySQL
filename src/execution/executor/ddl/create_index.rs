use crate::execution::executor::{BoxedExecutor, Executor};

use crate::planner::operator::create_index::CreateIndexOperator;
use crate::storage::Transaction;

use crate::types::tuple_builder::TupleBuilder;



pub struct CreateIndex {
    op: CreateIndexOperator,
}

impl From<CreateIndexOperator> for CreateIndex {
    fn from(op: CreateIndexOperator) -> Self {
        CreateIndex { op }
    }
}

impl<T: Transaction> Executor<T> for CreateIndex {
    fn execute(self, transaction: &mut T) -> BoxedExecutor {
        let CreateIndexOperator {
            table_name,
            index_name,
            col_name,
        } = self.op;
        let _ = transaction
            .create_index(table_name.clone(), index_name, &col_name)?;
        let tuple_builder = TupleBuilder::new_result();
        let tuple = tuple_builder
            .push_result("CREATE INDEX SUCCESS", format!("{}", table_name).as_str())?;
        Ok(vec![tuple])
    }
}
