use sqlparser::ast::{Ident, ObjectName, Statement};
mod create_table;

use crate::plan::LogicalPlan;
use crate::result::Result;
use crate::store::{StorageMut, Store};
use crate::{expression::ScalarExpression, plan::operator::join::JoinType, store::schema::Schema};
use std::collections::BTreeMap;

mod select;

type TableName = String;
pub(crate) static DEFAULT_DATABASE_NAME: &str = "kipsql";
pub(crate) static DEFAULT_SCHEMA_NAME: &str = "kipsql";

pub struct Storage<T: Store + StorageMut> {
    pub storage: T,
}



pub struct BinderContext<'a,S:Store + StorageMut> {
    storage:&'a S,
    pub(crate) bind_table: BTreeMap<TableName, (Schema, Option<JoinType>)>,
    aliases: BTreeMap<String, ScalarExpression>,
    table_aliases: BTreeMap<String, TableName>,
    group_by_exprs: Vec<ScalarExpression>,
    pub(crate) agg_calls: Vec<ScalarExpression>,
}
impl<'a,S:Store + StorageMut> BinderContext<'a,S>{
    pub fn new(storage: &'a S) -> Self {
        BinderContext {
            storage,
            bind_table: Default::default(),
            aliases: Default::default(),
            table_aliases: Default::default(),
            group_by_exprs: vec![],
            agg_calls: Default::default(),
        }
    }
}   


pub struct Binder<'a, S:Store+StorageMut> {
    context: BinderContext<'a,S>,
}

impl<'a, S:Store + StorageMut> Binder<'a, S> {
    pub fn new(context: BinderContext<'a, S>) -> Self {
        Binder { context }
    }
    pub async fn bind(mut self, stmt: &Statement) -> Result<LogicalPlan> {
        let plan = match stmt{
            // Statement::Query(query) => self.bind_query(query)?,
            Statement::CreateTable {
                name,
                columns,
                constraints,
                if_not_exists,
                ..
            } => self.bind_create_table(name, columns, constraints, *if_not_exists)?,

            _ => unimplemented!(),

        };
        todo!()
    }

}



#[derive(thiserror::Error, Debug)]
pub enum BindError {
    #[error("unsupported statement {0}")]
    UnsupportedStmt(String),
    #[error("invalid table {0}")]
    InvalidTable(String),
    #[error("invalid table name: {0:?}")]
    InvalidTableName(Vec<Ident>),
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
    // #[error("catalog error: {0}")]
    // CatalogError(#[from] CatalogError),
    // #[error("type error: {0}")]
    // TypeError(#[from] TypeError),
}



fn split_name(name: &ObjectName) -> Result<(&str, &str), BindError> {
    Ok(match name.0.as_slice() {
        [table] => (DEFAULT_SCHEMA_NAME, &table.value),
        [schema, table] => (&schema.value, &table.value),
        _ => return Err(BindError::InvalidTableName(name.0.clone())),
    })
}