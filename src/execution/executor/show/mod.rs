use std::sync::Arc;

use crate::{
    storage::Transaction,
    types::{tuple::Tuple, value::DataValue},
};

use super::{Executor, Source};

pub struct ShowTables;

impl<T: Transaction> Executor<T> for ShowTables {
    fn execute(self, transaction: &mut T) -> Source {
        let metas = transaction.show_tables()?;
        let mut tuples = Vec::new();
        for meta in metas {
            let values = vec![Arc::new(DataValue::Utf8(Some(meta)))];
            tuples.push(Tuple { id: None, values })
        }
        Ok(tuples)
    }
}
