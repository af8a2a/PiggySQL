
use sqlparser::ast::Statement;
use sqlparser::parser::ParserError;

use crate::binder::{BindError, Binder, BinderContext};

use crate::execution::executor::{build, BoxedExecutor};
use crate::execution::ExecutorError;

use crate::optimizer::{apply_optimization, OptimizerError};
use crate::parser;

use crate::storage::{Storage, StorageError, Transaction};
use crate::types::tuple::Tuple;

pub struct Database<S: Storage> {
    pub(crate) storage: S,
}

impl<S: Storage> Database<S> {
    /// Create a new Database instance.
    pub fn new(storage: S) -> Result<Self, DatabaseError> {
        Ok(Database { storage })
    }

    // /// Run SQL queries.
    pub async fn run(&self, sql: &str) -> Result<Vec<Tuple>, DatabaseError> {
        let mut transaction = self.storage.transaction()?;
        let tuples = Self::_run(sql, &mut transaction)?;

        transaction.commit()?;

        Ok(tuples?)
    }

    pub async fn new_transaction(&self) -> Result<DBTransaction<S>, DatabaseError> {
        let transaction = self.storage.transaction()?;

        Ok(DBTransaction { inner: transaction })
    }

    fn _run(
        sql: &str,
        transaction: &mut <S as Storage>::TransactionType,
    ) -> Result<BoxedExecutor, DatabaseError> {
        // parse
        let stmts = parser::parse(sql)?;
        if stmts.is_empty() {
            return Err(DatabaseError::EmptyStatement);
        }
        let mut binder = Binder::new(BinderContext::new(transaction));
        let source_plan = binder.bind(&stmts[0])?;
        // println!("source_plan plan: {:#?}", source_plan);
        let best_plan = apply_optimization(source_plan)?;
        // println!("best_plan plan: {:#?}", best_plan);

        Ok(build(best_plan, transaction))
    }


}

pub struct DBTransaction<S: Storage> {
    inner: S::TransactionType,
}

impl<S: Storage> DBTransaction<S> {
    pub async fn run(&mut self, sql: &str) -> Result<Vec<Tuple>, DatabaseError> {
        let stream = Database::<S>::_run(sql, &mut self.inner)?;
        Ok(stream?)
    }
    // pub fn run_with_stmt(&mut self, stmt: &Statement) -> Result<Vec<Tuple>, DatabaseError> {
    //     let mut binder = Binder::new(BinderContext::new(&self.inner));
    //     let source_plan = binder.bind(&stmt)?;
    //     // println!("source_plan plan: {:#?}", source_plan);
    //     let best_plan = apply_optimization(source_plan)?;
    //     // println!("best_plan plan: {:#?}", best_plan);

    //     let result = build(best_plan, &mut self.inner);

    //     // let stream = Database::<S>::_run(sql, &self.inner)?;
    //     Ok(result?)
    // }
    pub async fn commit(self) -> Result<(), DatabaseError> {
        self.inner.commit()?;

        Ok(())
    }
    pub async fn rollback(self) -> Result<(), DatabaseError> {
        self.inner.rollback()?;

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
    #[error("optimizer error: {0}")]
    OptimizerError(
        #[source]
        #[from]
        OptimizerError,
    ),
    #[error("transaction already exists")]
    TransactionAlreadyExists,
    #[error("no transaction begin")]
    NoTransactionBegin,

}

#[cfg(test)]
mod test {
    use crate::catalog::{ColumnCatalog, ColumnDesc};
    use crate::db::{Database, DatabaseError};
    use crate::storage::engine::memory::Memory;
    use crate::storage::{MVCCLayer, Storage, StorageError, Transaction};
    use crate::types::tuple::create_table;
    use crate::types::value::DataValue;
    use crate::types::LogicalType;
    use std::sync::Arc;
    use tempfile::TempDir;


    fn build_table(mut transaction: impl Transaction) -> Result<(), StorageError> {
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
                ColumnDesc::new(LogicalType::Integer, false, false, None),
                None,
            ),
        ];
        let _ = transaction.create_table(Arc::new("t1".to_string()), columns, false)?;
        transaction.commit()?;

        Ok(())
    }


    #[tokio::test]
    async fn test_transaction_sql() -> Result<(), DatabaseError> {
        let database = Database::new(MVCCLayer::new(Memory::new()))?;

        let mut tx_1 = database.new_transaction().await?;

        let _ = tx_1.run("create table t1 (a int primary key, b int)").await?;
        tx_1.run("create index test_index on t1 (b)").await?;
        let tuples = tx_1.run("explain select * from t1 where b>1").await?;
        for tuple in tuples {
            println!("{}", tuple);
        }
        tx_1.run("drop index t1.test_index").await?;
        let tuples = tx_1.run("explain select * from t1 where b>1").await?;
        for tuple in tuples {
            println!("{}", tuple);
        }

        // assert_eq!(
        //     tuples_1[0].values,
        //     vec![
        //         Arc::new(DataValue::Int32(Some(0))),
        //         Arc::new(DataValue::Int32(Some(0)))
        //     ]
        // );
        // assert_eq!(
        //     tuples_1[1].values,
        //     vec![
        //         Arc::new(DataValue::Int32(Some(1))),
        //         Arc::new(DataValue::Int32(Some(1)))
        //     ]
        // );
        // tx_1.commit()?;

        Ok(())
    }
    #[tokio::test]

    async fn test_crud_sql() -> Result<(), DatabaseError> {
        let database = Database::new(MVCCLayer::new(Memory::new()))?;

        let _ = database.run(
            "create table t1 (a int primary key, b int unique null, k int, z varchar unique null)",
        ).await?;
        let _ = database
            .run("create table t2 (c int primary key, d int unsigned null, e datetime)")
            .await?;
        let _ = database.run("insert into t1 (a, b, k, z) values (-99, 1, 1, 'k'), (-1, 2, 2, 'i'), (5, 3, 2, 'p'), (29, 4, 2, 'db')").await?;
        let _ = database.run("insert into t2 (d, c, e) values (2, 1, '2021-05-20 21:00:00'), (3, 4, '2023-09-10 00:00:00')").await?;
        let _ = database
            .run("create table t3 (a int primary key, b decimal(4,2))")
            .await?;
        let _ = database
            .run("insert into t3 (a, b) values (1, 1111), (2, 2.01), (3, 3.00)")
            .await?;
        let _ = database
            .run("insert into t3 (a, b) values (4, 4444), (5, 5222), (6, 1.00)")
            .await?;

        println!("full t1:");
        let tuples_full_fields_t1 = database.run("select * from t1").await?;
        println!("{}", create_table(&tuples_full_fields_t1));

        println!("full t2:");
        let tuples_full_fields_t2 = database.run("select * from t2").await?;
        println!("{}", create_table(&tuples_full_fields_t2));

        //todo
        println!("projection_and_filter:");
        let tuples_projection_and_filter = database.run("select a from t1 where b > 1").await?;
        println!("{}", create_table(&tuples_projection_and_filter));

        println!("projection_and_sort:");
        let tuples_projection_and_sort = database.run("select * from t1 order by a, b").await?;
        println!("{}", create_table(&tuples_projection_and_sort));

        println!("like t1 1:");
        let tuples_like_1_t1 = database.run("select * from t1 where z like '%k'").await?;
        println!("{}", create_table(&tuples_like_1_t1));

        println!("like t1 2:");
        let tuples_like_2_t1 = database.run("select * from t1 where z like '_b'").await?;
        println!("{}", create_table(&tuples_like_2_t1));

        println!("not like t1:");
        let tuples_not_like_t1 = database
            .run("select * from t1 where z not like '_b'")
            .await?;
        println!("{}", create_table(&tuples_not_like_t1));

        println!("in t1:");
        let tuples_in_t1 = database.run("select * from t1 where a in (5, 29)").await?;
        println!("{}", create_table(&tuples_in_t1));

        println!("not in t1:");
        let tuples_not_in_t1 = database
            .run("select * from t1 where a not in (5, 29)")
            .await?;
        println!("{}", create_table(&tuples_not_in_t1));

        println!("limit:");
        let tuples_limit = database.run("select * from t1 limit 1 offset 1").await?;
        println!("{}", create_table(&tuples_limit));

        println!("inner join:");
        let tuples_inner_join = database
            .run("select * from t1 inner join t2 on a = c")
            .await?;
        println!("{}", create_table(&tuples_inner_join));

        println!("left join:");
        let tuples_left_join = database
            .run("select * from t1 left join t2 on a = c")
            .await?;
        println!("{}", create_table(&tuples_left_join));

        println!("right join:");
        let tuples_right_join = database
            .run("select * from t1 right join t2 on a = c")
            .await?;
        println!("{}", create_table(&tuples_right_join));

        println!("full join:");
        let tuples_full_join = database
            .run("select * from t1 full join t2 on a = c")
            .await?;
        println!("{}", create_table(&tuples_full_join));

        println!("count agg:");
        let tuples_count_agg = database.run("select count(d) from t2").await?;
        println!("{}", create_table(&tuples_count_agg));

        println!("count wildcard agg:");
        let tuples_count_wildcard_agg = database.run("select count(*) from t2").await?;
        println!("{}", create_table(&tuples_count_wildcard_agg));

        println!("count distinct agg:");
        let tuples_count_distinct_agg = database.run("select count(distinct d) from t2").await?;
        println!("{}", create_table(&tuples_count_distinct_agg));

        println!("sum agg:");
        let tuples_sum_agg = database.run("select sum(d) from t2").await?;
        println!("{}", create_table(&tuples_sum_agg));

        println!("sum distinct agg:");
        let tuples_sum_distinct_agg = database.run("select sum(distinct d) from t2").await?;
        println!("{}", create_table(&tuples_sum_distinct_agg));

        println!("avg agg:");
        let tuples_avg_agg = database.run("select avg(d) from t2").await?;
        println!("{}", create_table(&tuples_avg_agg));

        println!("min_max agg:");
        let tuples_min_max_agg = database.run("select min(d), max(d) from t2").await?;
        println!("{}", create_table(&tuples_min_max_agg));

        println!("group agg:");
        let tuples_group_agg = database
            .run("select c, max(d) from t2 group by c having c = 1")
            .await?;
        println!("{}", create_table(&tuples_group_agg));

        println!("alias:");
        let tuples_group_agg = database.run("select c as o from t2").await?;
        println!("{}", create_table(&tuples_group_agg));

        println!("alias agg:");
        let tuples_group_agg = database
            .run("select c, max(d) as max_d from t2 group by c having c = 1")
            .await?;
        println!("{}", create_table(&tuples_group_agg));

        println!("time max:");
        let tuples_time_max = database.run("select max(e) as max_time from t2").await?;
        println!("{}", create_table(&tuples_time_max));

        println!("time where:");
        let tuples_time_where_t2 = database
            .run("select (c + 1) from t2 where e > '2021-05-20'")
            .await?;
        println!("{}", create_table(&tuples_time_where_t2));

        assert!(database
            .run("select max(d) from t2 group by c")
            .await
            .is_err());

        println!("distinct t1:");
        let tuples_distinct_t1 = database.run("select distinct b, k from t1").await?;
        println!("{}", create_table(&tuples_distinct_t1));

        println!("update t1 with filter:");
        let _ = database.run("update t1 set b = 0 where b = 1").await?;
        println!("after t1:");
        let update_after_full_t1 = database.run("select * from t1").await?;
        println!("{}", create_table(&update_after_full_t1));

        println!("insert overwrite t1:");
        let _ = database
            .run("insert overwrite t1 (a, b, k) values (-99, 1, 0)")
            .await?;
        println!("after t1:");
        let insert_overwrite_after_full_t1 = database.run("select * from t1").await?;
        println!("{}", create_table(&insert_overwrite_after_full_t1));

        assert!(database
            .run("insert overwrite t1 (a, b, k) values (-1, 1, 0)")
            .await
            .is_err());

        println!("delete t1 with filter:");
        let _ = database.run("delete from t1 where b = 0").await?;
        println!("after t1:");
        let delete_after_full_t1 = database.run("select * from t1").await?;
        println!("{}", create_table(&delete_after_full_t1));

        println!("drop t1:");
        let _ = database.run("drop table t1").await?;

        println!("decimal:");
        let tuples_decimal = database.run("select * from t3").await?;
        println!("{}", create_table(&tuples_decimal));

        Ok(())
    }
}
