use sqlparser::parser::ParserError;
use std::cell::RefCell;
use std::path::PathBuf;
use tracing::Level;

use crate::binder::{BindError, Binder, BinderContext};
use crate::execution::executor::{build, BoxedExecutor};
use crate::execution::ExecutorError;
use crate::parser::parse as parse_sql;
use crate::planner::LogicalPlan;
use crate::storage::kip_impl::KipStorage;
use crate::storage::{Storage, StorageError, Transaction};
use crate::types::tuple::Tuple;
use tracing_subscriber::FmtSubscriber;

pub struct Database<S: Storage> {
    pub(crate) storage: S,
}

impl Database<KipStorage> {
    /// Create a new Database instance With KipDB.
    pub async fn with_kipdb(path: impl Into<PathBuf> + Send) -> Result<Self, DatabaseError> {
        let storage = KipStorage::new(path).await?;
        Ok(Database { storage })
    }
}

impl<S: Storage> Database<S> {
    /// Create a new Database instance.
    pub fn new(storage: S) -> Result<Self, DatabaseError> {
        Ok(Database { storage })
    }

    /// Run SQL queries.
    pub async fn run(&self, sql: &str) -> Result<Vec<Tuple>, DatabaseError> {
        let transaction = self.storage.transaction().await?;
        let transaction = RefCell::new(transaction);
        // let mut stream = Self::_run(sql, &transaction)?;
        let tuples = Self::_run(sql, &transaction)?;

        transaction.into_inner().commit().await?;

        Ok(tuples?)
    }

    pub async fn new_transaction(&self) -> Result<DBTransaction<S>, DatabaseError> {
        let transaction = self.storage.transaction().await?;

        Ok(DBTransaction {
            inner: RefCell::new(transaction),
        })
    }

    fn _run(
        sql: &str,
        transaction: &RefCell<<S as Storage>::TransactionType>,
    ) -> Result<BoxedExecutor, DatabaseError> {
        // parse
        let stmts = parse_sql(sql).unwrap();
        if stmts.is_empty() {
            return Err(DatabaseError::EmptyStatement);
        }
        let binder = Binder::new(BinderContext::new(unsafe {
            transaction.as_ptr().as_ref().unwrap()
        }));
        /// Build a logical plan.
        ///
        /// SELECT a,b FROM t1 ORDER BY a LIMIT 1;
        /// Scan(t1)
        ///   Sort(a)
        ///     Limit(1)
        ///       Project(a,b)
        let source_plan = binder.bind(&stmts[0])?;
        // println!("source_plan plan: {:#?}", source_plan);

        // println!("best_plan plan: {:#?}", best_plan);

        Ok(build(source_plan, &transaction))
    }
}

pub struct DBTransaction<S: Storage> {
    inner: RefCell<S::TransactionType>,
}

impl<S: Storage> DBTransaction<S> {
    pub async fn run(&mut self, sql: &str) -> Result<Vec<Tuple>, DatabaseError> {
        let stream = Database::<S>::_run(sql, &self.inner)?;
        Ok(stream?)
    }

    pub async fn commit(self) -> Result<(), DatabaseError> {
        self.inner.into_inner().commit().await?;

        Ok(())
    }
}

#[derive(thiserror::Error, Debug)]
pub enum DatabaseError {
    #[error("sql statement is empty")]
    EmptyStatement,
    #[error("parse error: {0}")]
    Parse(
        #[source]
        #[from]
        ParserError,
    ),
    #[error("bind error: {0}")]
    Bind(
        #[source]
        #[from]
        BindError,
    ),
    #[error("Storage error: {0}")]
    StorageError(
        #[source]
        #[from]
        StorageError,
    ),
    #[error("executor error: {0}")]
    ExecutorError(
        #[source]
        #[from]
        ExecutorError,
    ),
    #[error("Internal error: {0}")]
    InternalError(String),
    // #[error("optimizer error: {0}")]
    // OptimizerError(
    //     #[source]
    //     #[from]
    //     OptimizerError,
    // ),
}

#[cfg(test)]
mod test {
    use crate::catalog::{ColumnCatalog, ColumnDesc};
    use crate::db::{Database, DatabaseError};
    use crate::storage::{Storage, StorageError, Transaction};
    use crate::types::tuple::create_table;
    use crate::types::value::DataValue;
    use crate::types::LogicalType;
    use std::sync::Arc;
    use tempfile::TempDir;
    use tracing::{debug, info};

    async fn build_table(mut transaction: impl Transaction) -> Result<(), StorageError> {
        let columns = vec![
            ColumnCatalog::new(
                "c1".to_string(),
                false,
                ColumnDesc::new(LogicalType::Integer, true, false, None),
                None,
            ),
            ColumnCatalog::new(
                "c2".to_string(),
                false,
                ColumnDesc::new(LogicalType::Boolean, false, false, None),
                None,
            ),
        ];
        let _ = transaction.create_table(Arc::new("t1".to_string()), columns, false)?;
        transaction.commit().await?;

        Ok(())
    }

    #[tokio::test]
    async fn test_run_sql() -> Result<(), DatabaseError> {
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");
        let database = Database::with_kipdb(temp_dir.path()).await?;
        let transaction = database.storage.transaction().await?;
        build_table(transaction).await?;

        let batch = database.run("select * from t1").await?;

        println!("{:#?}", batch);
        Ok(())
    }

    #[tokio::test]
    async fn test_transaction_sql() -> Result<(), DatabaseError> {
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");
        let kipsql = Database::with_kipdb(temp_dir.path()).await?;

        let mut tx_1 = kipsql.new_transaction().await?;

        let _ = tx_1
            .run("create table t1 (a int primary key, b int)")
            .await?;
        eprintln!("create table t1");
        let _ = tx_1.run("insert into t1 values(0, 0)").await?;
        let _ = tx_1.run("insert into t1 values(1, 1)").await?;
        eprintln!("insert into t1");
        // let _ = tx_2.run("insert into t1 values(2, 2)").await?;
        // let _ = tx_2.run("insert into t1 values(3, 3)").await?;

        let tuples_1 = tx_1.run("select * from t1").await?;
        eprintln!("tuple1 {:?}", tuples_1);
        eprintln!("tuple len {}", tuples_1.len());

        assert_eq!(tuples_1.len(), 2);

        assert_eq!(
            tuples_1[0].values,
            vec![
                Arc::new(DataValue::Int32(Some(0))),
                Arc::new(DataValue::Int32(Some(0)))
            ]
        );
        assert_eq!(
            tuples_1[1].values,
            vec![
                Arc::new(DataValue::Int32(Some(1))),
                Arc::new(DataValue::Int32(Some(1)))
            ]
        );
        tx_1.commit().await?;

        Ok(())
    }
}
