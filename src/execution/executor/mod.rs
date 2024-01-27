pub(crate) mod dql;
pub(crate) mod dml;
pub(crate) mod ddl;

use std::{cell::RefCell, sync::Arc};



use futures::stream::BoxStream;

use crate::{
    catalog::ColumnCatalog, storage::{StorageError, Transaction}, types::{tuple::Tuple, value::DataValue}
};

use super::ExecutorError;
pub type BoxedExecutor = BoxStream<'static, Result<Tuple, ExecutorError>>;

pub trait Executor<T: Transaction> {
    fn execute(self, transaction: &RefCell<T>) -> BoxedExecutor;
}
