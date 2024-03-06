use crate::errors::*;
use futures::pin_mut;
use futures::stream::TryStreamExt;
use rand::Rng;
use tokio_postgres::{Client, NoTls, Row, SimpleQueryMessage};
pub struct SQLClient {
    pub client: Client,
    pub sleep_ration: u64,
}

impl SQLClient {
    pub async fn connect() -> Result<Self> {
        let (client, connection) =
            tokio_postgres::connect("host=localhost user=postgres", NoTls).await?;
        tokio::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("connection error: {}", e);
            }
        });
        let sleep_ration = rand::thread_rng().gen_range(25..=75);
        Ok(Self {
            client,
            sleep_ration,
        })
    }
    pub async fn query_txn(&self, sql: Vec<String>) -> Result<Vec<Vec<SimpleQueryMessage>>> {
        for i in 0..32 {
            if i > 0 {
                println!("retry");

                tokio::time::sleep(std::time::Duration::from_millis(
                    2_u64.pow(i as u32 - 1) * self.sleep_ration,
                ))
                .await;
            }
            let result = async {
                self.client.simple_query("BEGIN").await?;
                let mut result = vec![];
                for sql in sql.iter() {
                    result.push(self.client.simple_query(sql).await?);
                }
                self.client.simple_query("COMMIT").await?;
                Ok(result)
            }
            .await;
            if result.is_err() {
                self.client.simple_query("ROLLBACK").await.ok();
                if matches!(result, Err(DatabaseError::Serialization)) {
                    continue;
                }
                continue;
            }
            return result;
        }

        Err(DatabaseError::Serialization)
    }
    pub async fn query(&self, sql: &str) -> Result<Vec<SimpleQueryMessage>> {
        for i in 0..16 {
            if i > 0 {
                tokio::time::sleep(std::time::Duration::from_millis(
                    2_u64.pow(i as u32 - 1) * rand::thread_rng().gen_range(25..=75),
                ))
                .await;
            }
            let result = async {
                self.client.simple_query("BEGIN").await?;
                let result = self.client.simple_query(sql).await?;
                self.client.simple_query("COMMIT").await?;
                Ok(result)
            }
            .await;
            if result.is_err() {
                self.client.simple_query("ROLLBACK").await.ok();
                continue;
            }
            return result;
        }

        Err(DatabaseError::Serialization)
    }
    pub async fn query_raw(&self, sql: &str) -> Result<Vec<Row>> {
        for i in 0..16 {
            if i > 0 {
                tokio::time::sleep(std::time::Duration::from_millis(
                    2_u64.pow(i as u32 - 1) * rand::thread_rng().gen_range(25..=75),
                ))
                .await;
            }
            self.client.simple_query("BEGIN").await?;
            let result = self.client.query_raw(sql, Vec::<String>::new()).await;
            match result {
                Ok(stream) => {
                    pin_mut!(stream);
                    let mut rows = vec![];
                    while let Some(row) = stream.try_next().await? {
                        rows.push(row);
                    }
                    self.client.simple_query("COMMIT").await?;
                    return Ok(rows);
                }
                Err(_) => {
                    self.client.simple_query("ROLLBACK").await.ok();
                    continue;
                }
            }
        }

        Err(DatabaseError::Serialization)
    }
}
