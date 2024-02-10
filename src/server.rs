use std::{io, sync::Arc};

use async_trait::async_trait;
use futures::stream;
use pgwire::{api::{auth::{noop::NoopStartupHandler, StartupHandler}, query::{ExtendedQueryHandler, PlaceholderExtendedQueryHandler, SimpleQueryHandler}, results::{DataRowEncoder, FieldFormat, FieldInfo, QueryResponse, Response, Tag}, ClientInfo, MakeHandler, StatelessMakeHandler, Type}, error::{ErrorInfo, PgWireError, PgWireResult}, tokio::process_socket};
use tokio::{net::TcpListener, sync::Mutex};

use crate::{
    db::{DBTransaction, Database, DatabaseError},
    storage::{engine::memory::Memory, MVCCLayer}, types::{tuple::Tuple, LogicalType},
};


#[derive(clap::Parser, Debug)]
#[command(author, version, about, long_about = None)]
pub struct Args {
    #[clap(long, default_value = "127.0.0.1")]
    pub ip: String,
    #[clap(long, default_value = "5432")]
    pub port: u16,

}


pub struct Session {
    inner: Arc<Database<MVCCLayer<Memory>>>,
    tx: Mutex<Option<DBTransaction<MVCCLayer<Memory>>>>,
}

pub struct Server {
    inner: Arc<Database<MVCCLayer<Memory>>>,
}




impl MakeHandler for Server {
    type Handler = Arc<Session>;

    fn make(&self) -> Self::Handler {
        Arc::new(Session {
            inner: Arc::clone(&self.inner),
            tx: Mutex::new(None),
        })
    }

}
#[async_trait]
impl SimpleQueryHandler for Session {
    async fn do_query<'a, 'b: 'a, C>(
        &'b self,
        _client: &mut C,
        query: &'a str,
    ) -> PgWireResult<Vec<Response<'a>>>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        match query.to_uppercase().as_str() {
            "BEGIN;" | "BEGIN" => {
                let mut guard = self.tx.lock().await;

                if guard.is_some() {
                    return Err(PgWireError::ApiError(Box::new(
                        DatabaseError::TransactionAlreadyExists,
                    )));
                }
                let transaction = self
                    .inner
                    .new_transaction()
                    .await
                    .map_err(|e| PgWireError::ApiError(Box::new(e)))?;
                guard.replace(transaction);

                Ok(vec![Response::Execution(Tag::new("OK").into())])
            }
            "COMMIT;" | "COMMIT" => {
                let mut guard = self.tx.lock().await;

                if let Some(transaction) = guard.take() {
                    transaction
                        .commit()
                        .await
                        .map_err(|e| PgWireError::ApiError(Box::new(e)))?;

                    Ok(vec![Response::Execution(Tag::new("OK").into())])
                } else {
                    Err(PgWireError::ApiError(Box::new(
                        DatabaseError::NoTransactionBegin,
                    )))
                }
            }
            "ROLLBACK;" | "ROLLBACK" => {
                let mut guard = self.tx.lock().await;

                if guard.is_none() {
                    return Err(PgWireError::ApiError(Box::new(
                        DatabaseError::NoTransactionBegin,
                    )));
                }
                drop(guard.take());

                Ok(vec![Response::Execution(Tag::new("OK").into())])
            }
            _ => {
                let mut guard = self.tx.lock().await;

                let tuples = if let Some(transaction) = guard.as_mut() {
                    transaction.run(query).await
                } else {
                    self.inner.run(query).await
                }
                .map_err(|e| PgWireError::ApiError(Box::new(e)))?;

                Ok(vec![Response::Query(encode_tuples(tuples)?)])
            }
        }
    }
}



impl Server {
    pub async fn new() -> Result<Server, DatabaseError> {
        let database = Database::new(MVCCLayer::new(Memory::new()))?;

        Ok(Server {
            inner: Arc::new(database),
        })
    }
}

fn encode_tuples<'a>(tuples: Vec<Tuple>) -> PgWireResult<QueryResponse<'a>> {
    if tuples.is_empty() {
        return Ok(QueryResponse::new(Arc::new(vec![]), stream::empty()));
    }

    let mut results = Vec::with_capacity(tuples.len());
    let schema = Arc::new(
        tuples[0]
            .columns
            .iter()
            .map(|column| {
                let pg_type = into_pg_type(column.datatype())?;

                Ok(FieldInfo::new(
                    column.name().into(),
                    None,
                    None,
                    pg_type,
                    FieldFormat::Text,
                ))
            })
            .collect::<PgWireResult<Vec<FieldInfo>>>()?,
    );

    for tuple in tuples {
        let mut encoder = DataRowEncoder::new(schema.clone());
        for value in tuple.values {
            match value.logical_type() {
                LogicalType::SqlNull => encoder.encode_field(&None::<i8>),
                LogicalType::Boolean => encoder.encode_field(&value.bool()),
                LogicalType::Tinyint => encoder.encode_field(&value.i8()),
                LogicalType::UTinyint => encoder.encode_field(&value.u8().map(|v| v as i8)),
                LogicalType::Smallint => encoder.encode_field(&value.i16()),
                LogicalType::USmallint => encoder.encode_field(&value.u16().map(|v| v as i16)),
                LogicalType::Integer => encoder.encode_field(&value.i32()),
                LogicalType::UInteger => encoder.encode_field(&value.u32()),
                LogicalType::Bigint => encoder.encode_field(&value.i64()),
                LogicalType::UBigint => encoder.encode_field(&value.u64().map(|v| v as i64)),
                LogicalType::Float => encoder.encode_field(&value.float()),
                LogicalType::Double => encoder.encode_field(&value.double()),
                LogicalType::Varchar(_) => encoder.encode_field(&value.utf8()),
                LogicalType::Date => encoder.encode_field(&value.date()),
                LogicalType::DateTime => encoder.encode_field(&value.datetime()),
                LogicalType::Decimal(_, _) => todo!(),
                _ => unreachable!(),
            }?;
        }

        results.push(encoder.finish());
    }

    Ok(QueryResponse::new(
        schema,
        stream::iter(results.into_iter()),
    ))
}
fn into_pg_type(data_type: &LogicalType) -> PgWireResult<Type> {
    Ok(match data_type {
        LogicalType::SqlNull => Type::UNKNOWN,
        LogicalType::Boolean => Type::BOOL,
        LogicalType::Tinyint | LogicalType::UTinyint => Type::CHAR,
        LogicalType::Smallint | LogicalType::USmallint => Type::INT2,
        LogicalType::Integer | LogicalType::UInteger => Type::INT4,
        LogicalType::Bigint | LogicalType::UBigint => Type::INT8,
        LogicalType::Float => Type::FLOAT4,
        LogicalType::Double => Type::FLOAT8,
        LogicalType::Varchar(_) => Type::VARCHAR,
        LogicalType::Date | LogicalType::DateTime => Type::DATE,
        LogicalType::Decimal(_, _) => todo!(),
        _ => {
            return Err(PgWireError::UserError(Box::new(ErrorInfo::new(
                "ERROR".to_owned(),
                "XX000".to_owned(),
                format!("Unsupported Datatype {data_type}"),
            ))));
        }
    })
}


