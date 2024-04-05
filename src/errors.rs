use std::{
    array::TryFromSliceError,
    num::{ParseFloatError, ParseIntError, TryFromIntError},
    str::ParseBoolError,
    string::FromUtf8Error,
};

use chrono::ParseError;
use sqlparser::parser::ParserError;
pub type Result<T> = std::result::Result<T, DatabaseError>;
use crate::{
    expression::BinaryOperator,
    types::{value::DataValue, LogicalType},
};

#[derive(thiserror::Error, Debug)]
pub enum DatabaseError {
    #[error("sql statement is empty")]
    EmptyStatement,
    #[error("invalid type")]
    InvalidType,
    #[error("must contain PrimaryKey!")]
    PrimaryKeyNotFound,
    #[error("not implemented sqlparser datatype: {0}")]
    NotImplementedSqlparserDataType(String),
    #[error("Type:{0},lhs:{1} cast to {2} fail")]
    CastFail(LogicalType, DataValue, LogicalType),
    #[error("too long")]
    TooLong,
    #[error("cannot be Null")]
    NotNull,
    #[error("try from int: {0}")]
    TryFromInt(
        #[source]
        #[from]
        TryFromIntError,
    ),
    #[error("parser int: {0}")]
    ParseInt(
        #[source]
        #[from]
        ParseIntError,
    ),
    #[error("parser bool: {0}")]
    ParseBool(
        #[source]
        #[from]
        ParseBoolError,
    ),
    #[error("parser float: {0}")]
    ParseFloat(
        #[source]
        #[from]
        ParseFloatError,
    ),
    #[error("parser date: {0}")]
    ParseDate(
        #[source]
        #[from]
        ParseError,
    ),
    #[error("parser sql: {0}")]
    ParserSql(
        #[source]
        #[from]
        ParserError,
    ),
    #[error("bindcode: {0}")]
    Bincode(
        #[source]
        #[from]
        Box<bincode::ErrorKind>,
    ),
    #[error("from utf8: {0}")]
    FromUtf8Error(
        #[source]
        #[from]
        FromUtf8Error,
    ),
    #[error("try from decimal")]
    TryFromDecimal(
        #[source]
        #[from]
        rust_decimal::Error,
    ),

    #[error("{0} and {1} do not match")]
    MisMatch(String, String),
    #[error("io: {0}")]
    IO(
        #[source]
        #[from]
        std::io::Error,
    ),
    #[error("the same primary key data already exists")]
    DuplicatePrimaryKey,
    #[error("the column has been declared unique and the value already exists")]
    DuplicateUniqueValue,
    #[error("the table not found")]
    TableNotFound,
    #[error("the some column already exists")]
    DuplicateColumn,
    #[error("add column must be nullable or specify a default value")]
    NeedNullAbleOrDefault,
    #[error("the table already exists")]
    TableExists,
    #[error("plan is empty")]
    EmptyPlan,
    #[error("this column must belong to a table")]
    OwnerLessColumn,
    #[error("there are more buckets: {0} than elements: {1}")]
    TooManyBuckets(usize, usize),
    #[error("tuple length mismatch: expected {expected} but got {actual}")]
    LengthMismatch { expected: usize, actual: usize },
    #[error("join error")]
    JoinError(
        #[from]
        #[source]
        tokio::task::JoinError,
    ),
    #[error("invalid index")]
    InvalidIndex,
    #[error("{0} not found: {1}")]
    NotFound(&'static str, String),
    #[error("duplicated {0}: {1}")]
    Duplicated(&'static str, String),
    #[error("columns empty")]
    ColumnsEmpty,
    #[error("unsupported statement {0}")]
    UnsupportedStmt(String),
    #[error("invalid table {0}")]
    InvalidTable(String),
    #[error("invalid column {0}")]
    InvalidColumn(String),
    #[error("ambiguous column {0}")]
    AmbiguousColumn(String),
    #[error("values length not match, expect {0}, got {1}")]
    ValuesLenMismatch(usize, usize),
    #[error("values list must all be the same length")]
    ValuesLenNotSame(),
    #[error("binary operator types mismatch: {0} != {1}")]
    BinaryOpTypeMismatch(String, String),
    #[error("subquery error: {0}")]
    Subquery(String),
    #[error("agg miss: {0}")]
    AggMiss(String),
    #[error("copy error: {0}")]
    UnsupportedCopySource(String),
    #[error("can not compare two types: {0} and {1}")]
    Incomparable(LogicalType, LogicalType),

    #[error("transaction already exists")]
    TransactionAlreadyExists,
    #[error("no transaction begin")]
    NoTransactionBegin,

    #[error("W-W conflict error")]
    Serialization,
    #[error("{0}")]
    InternalError(String),
    #[error("type cast error {0}")]
    CastError(
        #[from]
        #[source]
        TryFromSliceError,
    ),
    #[error("csv error: {0}")]
    Csv(
        #[from]
        #[source]
        csv::Error,
    ),
    #[error("the {0} cannot support {1} for calculations")]
    UnsupportedBinaryOperator(LogicalType, BinaryOperator),

    #[error("the DDL must run in serial,already running in: {0} transaction")]
    DDLSerialError(usize),

    #[error("Storage Error")]
    PiggyKVError(
        #[from]
        #[source]
        piggykv::error::KernelError,
    ),
}
