use crate::execution::executor::{BoxedExecutor, Executor};
use crate::execution::ExecutorError;
use crate::planner::operator::scan::ScanOperator;
use crate::storage::{Iter, Transaction};
use crate::types::errors::TypeError;
use crate::types::tuple::Tuple;

use std::cell::RefCell;

pub(crate) struct IndexScan {
    op: ScanOperator,
}

impl From<ScanOperator> for IndexScan {
    fn from(op: ScanOperator) -> Self {
        IndexScan { op }
    }
}

impl<T: Transaction> Executor<T> for IndexScan {
    fn execute(self, transaction: &RefCell<T>) -> BoxedExecutor {
        let ScanOperator {
            table_name,
            columns,
            limit,
            index_by,
            ..
        } = self.op;
        let transaction = transaction.borrow();

        let (index_meta, binaries) = index_by.ok_or(TypeError::InvalidType)?;
        let mut iter =
            transaction.read_by_index(table_name, limit, columns, index_meta, binaries)?;
        let mut tuples = Vec::new();
        while let Some(tuple) = iter.next_tuple()? {
            tuples.push(tuple);
            // yield tuple;
        }
        Ok(tuples)
        // unsafe { self._execute(transaction.as_ptr().as_ref().unwrap()) }
    }
}
