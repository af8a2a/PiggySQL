
use crate::execution::executor::{Executor, Source};

use crate::storage::Transaction;
use crate::types::tuple::Tuple;

pub struct Dummy {}

impl<T: Transaction> Executor<T> for Dummy {
    fn execute(self, _transaction: &mut T) -> Source {
        Ok(vec![Tuple {
            id: None,
            columns: vec![],
            values: vec![],
        }])
    }
}
