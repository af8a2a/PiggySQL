use std::{io, sync::Arc};

use async_trait::async_trait;
use futures::stream;
use rust_decimal::prelude::ToPrimitive;

use pgwire::{
    api::{
        auth::{noop::NoopStartupHandler, StartupHandler},
        portal::{Format, Portal},
        query::{ExtendedQueryHandler, SimpleQueryHandler, StatementOrPortal},
        results::{DataRowEncoder, DescribeResponse, FieldInfo, QueryResponse, Response, Tag},
        stmt::NoopQueryParser,
        ClientInfo, MakeHandler, StatelessMakeHandler, Type,
    },
    error::{ErrorInfo, PgWireError, PgWireResult},
    tokio::process_socket,
};
use tokio::{net::TcpListener, sync::Mutex};
use tracing::debug;

use crate::{
    catalog::SchemaRef,
    db::{DBTransaction, Database},
    errors::*,
    planner::operator::Operator,
    storage::{piggy_stroage::PiggyKVStroage},
    types::{tuple::Tuple, LogicalType},
};

pub struct Session {
    inner: Arc<Database<PiggyKVStroage>>,
    tx: Mutex<Option<DBTransaction<PiggyKVStroage>>>,
}
pub struct Server {
    pub(crate) inner: Arc<Database<PiggyKVStroage>>,
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
impl ExtendedQueryHandler for Session {
    type Statement = String;

    type QueryParser = NoopQueryParser;

    fn query_parser(&self) -> Arc<Self::QueryParser> {
        Arc::new(NoopQueryParser::new())
    }
    async fn do_describe<C>(
        &self,
        _client: &mut C,
        target: StatementOrPortal<'_, Self::Statement>,
    ) -> PgWireResult<DescribeResponse>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        match target {
            StatementOrPortal::Statement(stmt) => {
                let plan = self.inner.prepare_sql(&stmt.statement).await.unwrap();
                // debug!("plan: {:?}", plan);
                if let Operator::Project(op) = plan.operator {
                    let filed = op
                        .ouput_schema()
                        .iter()
                        .enumerate()
                        .map(|(idx, col)| {
                            let name = col.name();
                            let data_type = col.datatype();
                            FieldInfo::new(
                                name.to_string(),
                                None,
                                None,
                                into_pg_type(data_type).unwrap(),
                                Format::UnifiedText.format_for(idx),
                            )
                        })
                        .collect::<Vec<_>>();
                    // debug!("filed: {:?}", filed);
                    Ok(DescribeResponse::new(None, filed))
                } else {
                    Ok(DescribeResponse::new(None, vec![]))
                }
            }
            StatementOrPortal::Portal(portal) => {
                let plan = self
                    .inner
                    .prepare_sql(&portal.statement.statement)
                    .await
                    .unwrap();
                // debug!("plan: {:?}", plan);
                if let Operator::Project(op) = plan.operator {
                    let filed = op
                        .ouput_schema()
                        .iter()
                        .enumerate()
                        .map(|(idx, col)| {
                            let name = col.name();
                            let data_type = col.datatype();
                            FieldInfo::new(
                                name.to_string(),
                                None,
                                None,
                                into_pg_type(data_type).unwrap(),
                                Format::UnifiedText.format_for(idx),
                            )
                        })
                        .collect::<Vec<_>>();
                    // debug!("filed: {:?}", filed);
                    Ok(DescribeResponse::new(None, filed))
                } else {
                    Ok(DescribeResponse::new(None, vec![]))
                }
            }
        }
    }

    async fn do_query<'a, C>(
        &self,
        _client: &mut C,
        portal: &'a Portal<Self::Statement>,
        _max_rows: usize,
    ) -> PgWireResult<Response<'a>>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        let query = &portal.statement.statement;
        //  debug!("query: {}", query);
        match query.to_uppercase().as_str() {
            "BEGIN;" | "BEGIN" | "START TRANSACTION;" | "START TRANSACTION" => {
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

                Ok(Response::Execution(Tag::new("BEGIN")))
            }
            "COMMIT;" | "COMMIT" => {
                let mut guard = self.tx.lock().await;

                if let Some(transaction) = guard.take() {
                    transaction
                        .commit()
                        .await
                        .map_err(|e| PgWireError::ApiError(Box::new(e)))?;

                    Ok(Response::Execution(Tag::new("COMMIT")))
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

                Ok(Response::Execution(Tag::new("ROLLBACK")))
            }
            _ => {
                let mut guard = self.tx.lock().await;

                let (schema, tuples) = if let Some(transaction) = guard.as_mut() {
                    transaction.run(query).await
                } else {
                    self.inner.run(query).await
                }
                .map_err(|e| PgWireError::ApiError(Box::new(e)))?;
                // debug!("tuples: {:?}", tuples);
                Ok(Response::Query(encode_tuples(schema, tuples)?))
            }
        }
    }
}
fn row_desc_from_stmt(schema: SchemaRef, format: &Format) -> PgWireResult<Vec<FieldInfo>> {
    return schema
        .iter()
        .enumerate()
        .map(|(idx, col)| {
            let datatype = col.datatype();
            let name = col.name();

            Ok(FieldInfo::new(
                name.to_string(),
                None,
                None,
                into_pg_type(datatype).unwrap(),
                format.format_for(idx),
            ))
        })
        .collect();
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

                Ok(vec![Response::Execution(Tag::new("BEGIN"))])
            }
            "COMMIT;" | "COMMIT" => {
                let mut guard = self.tx.lock().await;

                if let Some(transaction) = guard.take() {
                    transaction
                        .commit()
                        .await
                        .map_err(|e| PgWireError::ApiError(Box::new(e)))?;

                    Ok(vec![Response::Execution(Tag::new("COMMIT"))])
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

                Ok(vec![Response::Execution(Tag::new("ROLLBACK"))])
            }
            _ => {
                let mut guard = self.tx.lock().await;

                let (schema, tuples) = if let Some(transaction) = guard.as_mut() {
                    transaction.run(query).await
                } else {
                    self.inner.run(query).await
                }
                .map_err(|e| PgWireError::ApiError(Box::new(e)))?;

                Ok(vec![Response::Query(encode_tuples(schema, tuples)?)])
            }
        }
    }
}

impl Server {
    pub async fn new(kv: PiggyKVStroage) -> Result<Arc<Server>> {
        Ok(Arc::new(Server {
            inner: Arc::new(Database::new(kv)?),
        }))
    }

    pub async fn run(server: Arc<Self>) {
        // let backend = Server::new().await.unwrap();
        let processor = server;

        // We have not implemented extended query in this server, use placeholder instead
        // let placeholder = Arc::new(StatelessMakeHandler::new(Arc::new(
        //     PlaceholderExtendedQueryHandler,
        // )));
        let authenticator = Arc::new(StatelessMakeHandler::new(Arc::new(NoopStartupHandler)));
        let server_addr = format!("{}:{}", "127.0.0.1", "5432");
        let listener = TcpListener::bind(server_addr).await.unwrap();

        tokio::select! {
            res = server_run(processor.clone(), processor.clone(), authenticator, listener) => {
                if let Err(err) = res {
                    debug!("server run error: {}", err);
                }
            }
            _ = quit() => {
                debug!("quit server");
            }
        }
    }
}

pub(crate) async fn server_run<
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
            if let Err(err) = process_socket(
                incoming_socket.0,
                None,
                authenticator_ref,
                processor_ref,
                placeholder_ref,
            )
            .await
            {
                debug!("process_socket error: {}", err);
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

fn encode_tuples<'a>(schema: SchemaRef, tuples: Vec<Tuple>) -> PgWireResult<QueryResponse<'a>> {
    // if tuples.is_empty() {
    //     return Ok(QueryResponse::new(Arc::new(vec![]), stream::empty()));
    // }
    let schema = Arc::new(row_desc_from_stmt(schema, &Format::UnifiedText)?);
    let mut results = Vec::with_capacity(tuples.len());

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
                LogicalType::Decimal(_, _) => {
                    encoder.encode_field(&value.decimal().map(|v| v.to_f64()))
                } //todo
                _ => unreachable!(),
            }?;
        }

        results.push(encoder.finish());
    }
    // debug!("results: {:?}", results);
    // debug!("schema: {:?}", schema);
    let iter = stream::iter(results);
    Ok(QueryResponse::new(schema, iter))
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
        LogicalType::Decimal(_, _) => Type::NUMERIC,
        _ => {
            return Err(PgWireError::UserError(Box::new(ErrorInfo::new(
                "ERROR".to_owned(),
                "XX000".to_owned(),
                format!("Unsupported Datatype {data_type}"),
            ))));
        }
    })
}
