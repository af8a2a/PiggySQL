use tokio_postgres::{Client, NoTls, SimpleQueryMessage};


pub struct SQLClient {
    pub client: Client,
}

impl SQLClient {
    pub async fn connect() -> Result<Self, tokio_postgres::Error> {
        let (client, connection) =
            tokio_postgres::connect("host=localhost user=postgres", NoTls).await?;
        tokio::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("connection error: {}", e);
            }
        });

        Ok(Self { client })
    }
    pub async fn query(&self, sql: &str) -> Result<Vec<SimpleQueryMessage>, tokio_postgres::Error> {
        self.client.simple_query(sql).await
    }
}
