use std::time::Instant;

use piggysql::{
    db::Database,
    errors::DatabaseError,
    storage::{
        engine::{memory::Memory, StorageEngine},
        MVCCLayer,
    },
};
use sqllogictest::{AsyncDB, DBOutput, DefaultColumnType};

pub struct Mock<S: StorageEngine> {
    pub db: Database<MVCCLayer<S>>,
}

impl Mock<Memory> {
    pub fn new() -> Self {
        Self {
            db: Database::new_memory().unwrap(),
        }
    }
}
#[async_trait::async_trait]
impl AsyncDB for Mock<Memory> {
    type Error = DatabaseError;

    type ColumnType = DefaultColumnType;

    async fn run(
        &mut self,
        sql: &str,
    ) -> Result<sqllogictest::DBOutput<Self::ColumnType>, Self::Error> {
        let start = Instant::now();
        let tuples = self.db.run(sql).await?;
        println!("|— Input SQL:");
        println!(" |— {}", sql);
        println!(" |— Time consuming: {:?}", start.elapsed());
        if tuples.is_empty() {
            return Ok(DBOutput::StatementComplete(0));
        }
        let types = vec![DefaultColumnType::Any; tuples[0].columns.len()];
        let rows = tuples
            .into_iter()
            .map(|tuple| {
                tuple
                    .values
                    .into_iter()
                    .map(|value| format!("{}", value))
                    .collect()
            })
            .collect();
        Ok(DBOutput::Rows { types, rows })
    }
}
