pub mod aggregate_function;
pub mod ast_literal;
pub mod ddl;
pub mod error;
pub mod expr;
pub mod operator;
pub mod query;
pub mod types;

use crate::{
    ast::{
        expr::Expr,
        query::{OrderByExpr, Query},
        Assignment,
    },
    result::Error,
};

use self::{
    ddl::{translate_alter_table_operation, translate_column_def},
    error::TranslateError,
};
use {
    crate::{ast::Statement, result::Result},
    sqlparser::ast::{
        Expr as SqlExpr, Ident as SqlIdent, ObjectName as SqlObjectName,
        ObjectType as SqlObjectType, Statement as SqlStatement, TableFactor, TableWithJoins,
    },
};

impl Statement {
    pub fn from(sql_statement: &SqlStatement) -> Result<Self> {
        match sql_statement {
            SqlStatement::Query(query) => Ok(Statement::Query(Query::from(query)?)),
            SqlStatement::Insert {
                table_name,
                columns,
                source,
                ..
            } => Ok(Statement::Insert {
                table_name: translate_object_name(table_name)?,
                columns: translate_idents(columns),
                source: Query::from(source)?,
            }),
            SqlStatement::Update {
                table,
                assignments,
                selection,
                ..
            } => Ok(Statement::Update {
                table_name: translate_table_with_join(table)?,
                assignments: assignments
                    .iter()
                    .map(Assignment::from)
                    .collect::<Result<_>>()?,
                selection: selection.as_ref().map(Expr::from).transpose()?,
            }),
            SqlStatement::Delete {
                from,
                selection,
                ..
            } => {
                let table_name = from
                    .iter()
                    .map(translate_table_with_join)
                    .next()
                    .ok_or(TranslateError::UnreachableEmptyTable)??;
                Ok(Statement::Delete {
                    table_name,
                    selection: selection.as_ref().map(Expr::from).transpose()?,
                })
            }
            //todo
            SqlStatement::CreateTable {
                if_not_exists,
                name,
                columns,
                query,
                engine,
                ..
            } => {
                let columns = columns
                    .iter()
                    .map(translate_column_def)
                    .collect::<Result<Vec<_>>>()?;

                let columns = (!columns.is_empty()).then_some(columns);

                Ok(Statement::CreateTable {
                    if_not_exists: *if_not_exists,
                    name: translate_object_name(name)?,
                    columns,
                    source: match query {
                        Some(v) => Some(Query::from(v).map(Box::new)?),
                        None => None,
                    },
                    engine: engine.clone(),
                })
            }

            SqlStatement::CreateIndex {
                name,
                table_name,
                columns,
                ..
            } => {
                if columns.len() > 1 {
                    return Err(TranslateError::CompositeIndexNotSupported.into());
                }

                let Some(name) = name else {
                    return Err(TranslateError::UnsupportedUnnamedIndex.into());
                };

                let name = translate_object_name(name)?;

                if name.to_uppercase() == "PRIMARY" {
                    return Err(TranslateError::ReservedIndexName(name).into());
                };

                Ok(Statement::CreateIndex {
                    name,
                    table_name: translate_object_name(table_name)?,
                    column: OrderByExpr::from(&columns[0])?,
                })
            }

            SqlStatement::AlterTable {
                name, operations, ..
            } => {
                if operations.len() > 1 {
                    return Err(TranslateError::UnsupportedMultipleAlterTableOperations.into());
                }

                let operation = operations
                    .iter()
                    .next()
                    .ok_or(TranslateError::UnreachableEmptyAlterTableOperation)?;

                Ok(Statement::AlterTable {
                    name: translate_object_name(name)?,
                    operation: translate_alter_table_operation(operation)?,
                })
            }
            SqlStatement::AlterIndex { name, operation } => todo!(),
            SqlStatement::Drop {
                object_type: SqlObjectType::Table,
                if_exists,
                names,
                ..
            } => Ok(Statement::DropTable {
                if_exists: *if_exists,
                names: names
                    .iter()
                    .map(translate_object_name)
                    .collect::<Result<Vec<_>>>()?,
            }),

            SqlStatement::ShowTables {
                ..
            } => todo!(),
            SqlStatement::StartTransaction { .. } => Ok(Statement::StartTransaction),
            SqlStatement::Commit { .. } => Ok(Statement::Commit),
            SqlStatement::Rollback { .. } => Ok(Statement::Rollback),
            SqlStatement::Explain {
                ..
            } => todo!(),
            _ => unimplemented!(),
        }
    }
}

fn translate_table_with_join(table: &TableWithJoins) -> Result<String> {
    if !table.joins.is_empty() {
        return Err(TranslateError::JoinOnUpdateNotSupported.into());
    }
    match &table.relation {
        TableFactor::Table { name, .. } => translate_object_name(name),
        t => Err(TranslateError::UnsupportedTableFactor(t.to_string()).into()),
    }
}

fn translate_object_name(sql_object_name: &SqlObjectName) -> Result<String> {
    let sql_object_name = &sql_object_name.0;
    if sql_object_name.len() > 1 {
        let compound_object_name = translate_idents(sql_object_name).join(".");
        return Err(TranslateError::CompoundObjectNotSupported(compound_object_name).into());
    }

    sql_object_name
        .get(0)
        .map(|v| v.value.to_owned())
        .ok_or_else(|| TranslateError::UnreachableEmptyObject.into())
}

pub fn translate_idents(idents: &[SqlIdent]) -> Vec<String> {
    idents.iter().map(|v| v.value.to_owned()).collect()
}
