use crate::execution::executor::{Source, Executor};

use crate::planner::operator::scan::ScanOperator;
use crate::storage::{Iter, Transaction};






pub(crate) struct SeqScan {
    op: ScanOperator,
}

impl From<ScanOperator> for SeqScan {
    fn from(op: ScanOperator) -> Self {
        SeqScan { op }
    }
}

impl<T: Transaction> Executor<T> for SeqScan {
    fn execute(self, transaction: &mut T) -> Source {
        let ScanOperator {
            table_name,
            columns,
            
            ..
        } = self.op;
        let mut iter = transaction.read(table_name,  columns)?;
        // let mut tuples = Vec::new();
        let tuples = iter.fetch_tuple()?.unwrap_or(vec![]);
        // while let Some(tuple) = iter.next_tuple()? {
        //     tuples.push(tuple);
        // }
        Ok(tuples)
        // unsafe { self._execute(transaction.as_ptr().as_ref().unwrap()) }
    }
}

