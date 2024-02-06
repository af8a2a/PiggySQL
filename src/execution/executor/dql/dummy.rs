use crate::execution::executor::{BoxedExecutor, Executor};

use crate::storage::Transaction;




pub struct Dummy {}

impl<T: Transaction> Executor<T> for Dummy {
    fn execute<'a>(self, _transaction: &mut T) -> BoxedExecutor {
        // self._execute()
        Ok(vec![])
    }
}

impl Dummy {
    pub async fn _execute(self) {}
}
