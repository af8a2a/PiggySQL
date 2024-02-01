use crate::execution::executor::{BoxedExecutor, Executor};

use crate::planner::operator::create_table::CreateTableOperator;
use crate::storage::Transaction;

use crate::types::tuple_builder::TupleBuilder;


use std::cell::RefCell;

pub struct CreateTable {
    op: CreateTableOperator,
}

impl From<CreateTableOperator> for CreateTable {
    fn from(op: CreateTableOperator) -> Self {
        CreateTable { op }
    }
}

impl<T: Transaction> Executor<T> for CreateTable {
    fn execute(self, transaction: &RefCell<T>) -> BoxedExecutor {
        let CreateTableOperator {
            table_name,
            columns,
            if_not_exists,
        } = self.op;
        let _ =
            transaction
                .borrow_mut()
                .create_table(table_name.clone(), columns, if_not_exists)?;
        let tuple_builder = TupleBuilder::new_result();
        let tuple = tuple_builder
            .push_result("CREATE TABLE SUCCESS", format!("{}", table_name).as_str())?;
        Ok(vec![tuple])
    }
}
