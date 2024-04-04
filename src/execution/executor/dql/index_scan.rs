use crate::execution::executor::{Executor, Source};

use crate::errors::*;
use crate::planner::operator::scan::ScanOperator;
use crate::storage::{StorageIter, Transaction};

pub(crate) struct IndexScan {
    op: ScanOperator,
}

impl From<ScanOperator> for IndexScan {
    fn from(op: ScanOperator) -> Self {
        IndexScan { op }
    }
}

impl<T: Transaction> Executor<T> for IndexScan {
    fn execute(self, transaction: &mut T) -> Source {
        let ScanOperator {
            table_name,
            columns,

            index_by,
            ..
        } = self.op;

        let (index_meta, binaries) = index_by.ok_or(DatabaseError::InvalidType)?;
        let mut iter = transaction.read_by_index(table_name, columns, index_meta, binaries)?;
        let tuples = iter.fetch_tuple()?;
        match tuples {
            Some(tuple) => Ok(tuple),
            None => Ok(vec![]),
        }
    }
}
