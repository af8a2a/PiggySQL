use pgwire::api::query;

use crate::binder::{Binder, BinderContext};

use crate::execution::executor::{build, Source};

use crate::optimizer::apply_optimization;
use crate::parser;

use crate::errors::{DatabaseError, Result};
use crate::storage::engine::memory::Memory;
use crate::storage::{MVCCLayer, Storage, Transaction};
use crate::types::tuple::Tuple;
pub struct Database<S: Storage> {
    pub(crate) storage: S,
}

impl Database<MVCCLayer<Memory>> {
    pub fn new_memory() -> Result<Self> {
        Ok(Database {
            storage: MVCCLayer::new_memory(),
        })
    }
}

impl<S: Storage> Database<S> {
    /// Create a new Database instance.
    pub fn new(storage: S) -> Result<Self> {
        Ok(Database { storage })
    }

    // /// Run SQL queries.
    pub async fn run(&self, sql: &str) -> Result<Vec<Tuple>> {
        let mut transaction = self.storage.transaction().await?;
        let tuples = Self::_run(sql, &mut transaction)?;

        transaction.commit()?;

        Ok(tuples?)
    }

    pub async fn new_transaction(&self) -> Result<DBTransaction<S>> {
        let transaction = self.storage.transaction().await?;

        Ok(DBTransaction {
            inner: transaction,
            query_list: vec![],
        })
    }

    fn _run(sql: &str, transaction: &mut <S as Storage>::TransactionType) -> Result<Source> {
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
    query_list: Vec<String>,
    inner: S::TransactionType,
}

impl<S: Storage> DBTransaction<S> {
    pub async fn run(&mut self, sql: &str) -> Result<Vec<Tuple>> {
        self.query_list.push(sql.to_string());
        Database::<S>::_run(sql, &mut self.inner)?
        
    }
    pub async fn commit(self) -> Result<()> {
        self.inner.commit()?;

        Ok(())
    }
    pub async fn rollback(self) -> Result<()> {
        self.inner.rollback()?;

        Ok(())
    }
}

#[cfg(test)]
mod test {
    use super::*;

    use crate::db::Database;
    use crate::storage::engine::memory::Memory;
    use crate::storage::MVCCLayer;
    use crate::types::tuple::create_table;
    use crate::types::value::DataValue;

    use std::sync::Arc;
    #[tokio::test]
    async fn test_transaction_sql() -> Result<()> {
        let database = Database::new(MVCCLayer::new(Memory::new()))?;

        database
            .run("create table halloween (id int primary key,salary int)")
            .await?;
        database
            .run("insert into halloween values (1,1000), (2,2000), (3,3000), (4,4000)")
            .await?;
        database
            .run("update halloween set salary = salary + 1000 where salary < 3000")
            .await?;
        let tuple = database.run("select salary from halloween;").await?;
        assert_eq!(tuple.len(), 4);
        assert_eq!(tuple[0].values[0], Arc::new(DataValue::Int32(Some(2000))));
        assert_eq!(tuple[1].values[0], Arc::new(DataValue::Int32(Some(3000))));
        assert_eq!(tuple[2].values[0], Arc::new(DataValue::Int32(Some(3000))));
        assert_eq!(tuple[3].values[0], Arc::new(DataValue::Int32(Some(4000))));

        Ok(())
    }
    #[tokio::test]

    async fn test_crud_sql() -> Result<()> {
        let database = Database::new(MVCCLayer::new_memory())?;

        let _ = database.run(
            "create table t1 (a int primary key, b int unique null, k int, z varchar unique null)",
        ).await?;
        let _ = database
            .run("create table t2 (c int primary key, d int unsigned null, e datetime)")
            .await?;
        let _ = database.run("insert into t1 (a, b, k, z) values (-99, 1, 1, 'k'), (-1, 2, 2, 'i'), (5, 3, 2, 'p'), (29, 4, 2, 'db')").await?;
        let _ = database.run("insert into t2 (d, c, e) values (2, 1, '2021-05-20 21:00:00'), (3, 4, '2023-09-10 00:00:00')").await?;

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

        Ok(())
    }
}
