use std::{io, sync::Arc};

use async_trait::async_trait;
use clap::Parser;
use futures::stream;
use pgwire::{
    api::{
        auth::{noop::NoopStartupHandler, StartupHandler},
        query::{ExtendedQueryHandler, PlaceholderExtendedQueryHandler, SimpleQueryHandler},
        results::{DataRowEncoder, FieldFormat, FieldInfo, QueryResponse, Response, Tag},
        ClientInfo, MakeHandler, StatelessMakeHandler, Type,
    },
    error::{ErrorInfo, PgWireError, PgWireResult},
    tokio::process_socket,
};
use tokio::{net::TcpListener, sync::Mutex};

use crate::{
    db::{DBTransaction, Database},
    errors::*,
    storage::{engine::memory::Memory, MVCCLayer},
    types::{tuple::Tuple, LogicalType},
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
    async fn new() -> Result<Server> {
        let database = Database::new(MVCCLayer::new(Memory::new()))?;

        Ok(Server {
            inner: Arc::new(database),
        })
    }
    pub async fn run() {
        let args = Args::parse();

        let backend = Server::new().await.unwrap();
        let processor = Arc::new(backend);
        // We have not implemented extended query in this server, use placeholder instead
        let placeholder = Arc::new(StatelessMakeHandler::new(Arc::new(
            PlaceholderExtendedQueryHandler,
        )));
        let authenticator = Arc::new(StatelessMakeHandler::new(Arc::new(NoopStartupHandler)));
        let server_addr = format!("{}:{}", args.ip, args.port);
        let listener = TcpListener::bind(server_addr).await.unwrap();

        tokio::select! {
            res = server_run(processor, placeholder, authenticator, listener) => {
                if let Err(_err) = res {
                }
            }
            _ = quit() => {}
        }
    }
}

async fn server_run<
    A: MakeHandler<Handler = Arc<impl StartupHandler + 'static>>,
    Q: MakeHandler<Handler = Arc<impl SimpleQueryHandler + 'static>>,
    EQ: MakeHandler<Handler = Arc<impl ExtendedQueryHandler + 'static>>,
>(
    processor: Arc<Q>,
    placeholder: Arc<EQ>,
    authenticator: Arc<A>,
    listener: TcpListener,
) -> io::Result<()> {
    loop {
        let incoming_socket = listener.accept().await?;
        let authenticator_ref = authenticator.make();
        let processor_ref = processor.make();
        let placeholder_ref = placeholder.make();

        tokio::spawn(async move {
            if let Err(_err) = process_socket(
                incoming_socket.0,
                None,
                authenticator_ref,
                processor_ref,
                placeholder_ref,
            )
            .await
            {
                {}
            }
        });
    }
}

async fn quit() -> io::Result<()> {
    #[cfg(unix)]
    {
        let mut interrupt =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt())?;
        let mut terminate =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
        tokio::select! {
            _ = interrupt.recv() => (),
            _ = terminate.recv() => (),
        }
        Ok(())
    }
    #[cfg(windows)]
    {
        let mut signal = tokio::signal::windows::ctrl_c()?;
        let _ = signal.recv().await;

        Ok(())
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
        _ => {
            return Err(PgWireError::UserError(Box::new(ErrorInfo::new(
                "ERROR".to_owned(),
                "XX000".to_owned(),
                format!("Unsupported Datatype {data_type}"),
            ))));
        }
    })
}
