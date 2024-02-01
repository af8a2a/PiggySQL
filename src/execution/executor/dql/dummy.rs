use crate::execution::executor::{BoxedExecutor, Executor};
use crate::execution::ExecutorError;
use crate::storage::Transaction;
use crate::types::tuple::Tuple;

use std::cell::RefCell;

pub struct Dummy {}

impl<T: Transaction> Executor<T> for Dummy {
    fn execute<'a>(self, _transaction: &RefCell<T>) -> BoxedExecutor {
        // self._execute()
        Ok(vec![])
    }
}

impl Dummy {
    pub async fn _execute(self) {}
}
