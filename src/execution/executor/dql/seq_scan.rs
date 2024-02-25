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
            limit,
            ..
        } = self.op;
        let mut iter = transaction.read(table_name,limit, columns)?;
        // let mut tuples = Vec::new();
        let tuples = iter.fetch_tuple()?.unwrap_or(vec![]);
        Ok(tuples)
    }
}

