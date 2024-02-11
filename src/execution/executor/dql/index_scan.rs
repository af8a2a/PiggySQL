use crate::execution::executor::{BoxedExecutor, Executor};

use crate::planner::operator::scan::ScanOperator;
use crate::storage::{Iter, Transaction};
use crate::errors::*;




pub(crate) struct IndexScan {
    op: ScanOperator,
}

impl From<ScanOperator> for IndexScan {
    fn from(op: ScanOperator) -> Self {
        IndexScan { op }
    }
}

impl<T: Transaction> Executor<T> for IndexScan {
    fn execute(self, transaction: &mut T) -> BoxedExecutor {
        let ScanOperator {
            table_name,
            columns,
            
            index_by,
            ..
        } = self.op;

        let (index_meta, binaries) = index_by.ok_or(DatabaseError::InvalidType)?;
        let mut iter =
            transaction.read_by_index(table_name, columns, index_meta, binaries)?;
        let tuples = iter.fetch_tuple()?.expect("unwrap tuple error");
        Ok(tuples)
    }
}
