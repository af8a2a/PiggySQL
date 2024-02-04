use crate::execution::executor::{BoxedExecutor, Executor};

use crate::planner::operator::scan::ScanOperator;
use crate::storage::{Iter, Transaction};


use std::cell::RefCell;



pub(crate) struct SeqScan {
    op: ScanOperator,
}

impl From<ScanOperator> for SeqScan {
    fn from(op: ScanOperator) -> Self {
        SeqScan { op }
    }
}

impl<T: Transaction> Executor<T> for SeqScan {
    fn execute(self, transaction: &RefCell<T>) -> BoxedExecutor {
        let ScanOperator {
            table_name,
            columns,
            limit,
            ..
        } = self.op;
        let transaction=transaction.borrow();
        let mut iter = transaction.read(table_name, limit, columns)?;
        // let mut tuples = Vec::new();
        let tuples = iter.fetch_tuple()?.unwrap_or(vec![]);
        // while let Some(tuple) = iter.next_tuple()? {
        //     tuples.push(tuple);
        // }
        Ok(tuples)
        // unsafe { self._execute(transaction.as_ptr().as_ref().unwrap()) }
    }
}

