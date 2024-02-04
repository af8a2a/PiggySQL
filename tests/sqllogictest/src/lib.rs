use std::time::Instant;

use piggysql::{
    db::{Database, DatabaseError},
    storage::{
        engine::{memory::Memory, StorageEngine},
        mvcc::MVCC,
        MVCCLayer,
    },
};
use sqllogictest::{DBOutput, DefaultColumnType};

pub struct Mock<S: StorageEngine> {
    pub db: Database<MVCCLayer<S>>,
}

impl Mock<Memory> {
    pub fn new() -> Self {
        Self {
            db: Database::new(MVCCLayer::new(Memory::new())).unwrap(),
        }
    }
}

impl sqllogictest::DB for Mock<Memory> {
    type Error = DatabaseError;

    type ColumnType = DefaultColumnType;

    fn run(&mut self, sql: &str) -> Result<sqllogictest::DBOutput<Self::ColumnType>, Self::Error> {
        let start = Instant::now();
        let tuples = self.db.run(sql)?;
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
