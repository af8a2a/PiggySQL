use std::sync::Arc;

use crate::{
    catalog::ColumnCatalog,
    storage::Transaction,
    types::{tuple::Tuple,  value::DataValue},
};

use super::{Source, Executor};

pub struct ShowTables;

impl<T: Transaction> Executor<T> for ShowTables {
    fn execute(self, transaction: &mut T) -> Source {
        let metas = transaction.show_tables()?;
        let mut tuples = Vec::new();
        for meta in metas {
            let columns = vec![Arc::new(ColumnCatalog::new_dummy("TABLE".to_string()))];
            let values = vec![Arc::new(DataValue::Utf8(Some(meta)))];
            tuples.push(Tuple {
                id: None,
                columns,
                values,
            })
        }
        Ok(tuples)
    }
}
