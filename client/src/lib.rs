use anyhow::{bail, Context, Result};
use comfy_table::{Cell, Table};
use rand::Rng;
use tokio_postgres::{Client, NoTls, Row, SimpleQueryMessage};

pub struct SQLClient {
    pub client: Client,
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
        Ok(Self { client })
    }
    pub async fn query(&self, sql: &str) -> Result<Vec<SimpleQueryMessage>> {
        for i in 0..16 {
            if i > 0 {
                tokio::time::sleep(std::time::Duration::from_millis(
                    2_u64.pow(i as u32 - 1) * rand::thread_rng().gen_range(25..=75),
                ))
                .await;
            }
            let result = self
                .client
                .simple_query(sql)
                .await
                .context("sql query error");
            return result;
        }
        bail!("Serialization verfication failed")
    }
}

pub fn create_table(rows: Vec<SimpleQueryMessage>) -> Table {
    let mut table = Table::new();

    if rows.is_empty() {
        return table;
    }

    let mut show_schema = false;
    for row in rows {
        if let SimpleQueryMessage::Row(row) = row {
            if !show_schema {
                let mut header = Vec::new();
                show_schema = true;
                for col in row.columns() {
                    header.push(Cell::new(col.name().to_string()));
                }
                table.set_header(header);
            }

            let mut cols = vec![];
            for idx in 0..row.len() {
                let val = row.get(idx);
                cols.push(val);
            }
            let cells = cols
                .iter()
                .map(|value| match value {
                    Some(value) => Cell::new(format!("{value}")),
                    None => Cell::new(format!("null")),
                })
                .collect::<Vec<_>>();
            table.add_row(cells);
        }
    }

    table
}
