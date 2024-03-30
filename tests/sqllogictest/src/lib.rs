use std::time::Instant;

use piggysql::{
    db::Database,
    errors::DatabaseError,
    storage::{piggy_stroage::PiggyKVStroage, Storage},
};
use sqllogictest::{AsyncDB, DBOutput, DefaultColumnType};

pub struct Mock<S: Storage> {
    pub db: Database<S>,
}

// impl Mock<MVCCLayer<Memory>> {
//     pub fn new() -> Self {
//         Self {
//             db: Database::new_memory().unwrap(),
//         }
//     }
// }
impl Mock<PiggyKVStroage> {
    pub fn new_lsm(path: std::path::PathBuf) -> Self {
        Self {
            db: Database::new_lsm(path).unwrap(),
        }
    }
}
#[async_trait::async_trait]
impl AsyncDB for Mock<PiggyKVStroage> {
    type Error = DatabaseError;

    type ColumnType = DefaultColumnType;

    async fn run(
        &mut self,
        sql: &str,
    ) -> Result<sqllogictest::DBOutput<Self::ColumnType>, Self::Error> {
        let start = Instant::now();
        let (schema, tuples) = self.db.run(sql).await?;
        println!("|— Input SQL:");
        println!(" |— {}", sql);
        println!(" |— Time consuming: {:?}", start.elapsed());
        if tuples.is_empty() {
            return Ok(DBOutput::StatementComplete(0));
        }
        let types = vec![DefaultColumnType::Any; schema.len()];
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
