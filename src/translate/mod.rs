pub mod aggregate_function;
pub mod ast_literal;
pub mod ddl;
pub mod error;
pub mod expr;
pub mod operator;
pub mod query;
pub mod types;

use crate::{
    ast::{expr::Expr, query::Query, Assignment},
    result::Error,
};

use self::error::TranslateError;
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
                tables,
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
            SqlStatement::CreateTable {
                or_replace,
                temporary,
                external,
                global,
                if_not_exists,
                transient,
                name,
                columns,
                constraints,
                hive_distribution,
                hive_formats,
                table_properties,
                with_options,
                file_format,
                location,
                query,
                without_rowid,
                like,
                clone,
                engine,
                comment,
                auto_increment_offset,
                default_charset,
                collation,
                on_commit,
                on_cluster,
                order_by,
                strict,
            } => todo!(),

            SqlStatement::CreateIndex {
                name,
                table_name,
                using,
                columns,
                unique,
                concurrently,
                if_not_exists,
                include,
                nulls_distinct,
                predicate,
            } => todo!(),

            SqlStatement::AlterTable {
                name,
                if_exists,
                only,
                operations,
            } => todo!(),
            SqlStatement::AlterIndex { name, operation } => todo!(),
            SqlStatement::Drop {
                object_type,
                if_exists,
                names,
                cascade,
                restrict,
                purge,
                temporary,
            } => todo!(),

            SqlStatement::ShowTables {
                extended,
                full,
                db_name,
                filter,
            } => todo!(),
            SqlStatement::StartTransaction {
                modes,
                begin,
                modifier,
            } => todo!(),
            SqlStatement::SetTransaction {
                modes,
                snapshot,
                session,
            } => todo!(),
            SqlStatement::Comment {
                object_type,
                object_name,
                comment,
                if_exists,
            } => todo!(),
            SqlStatement::Commit { chain } => todo!(),
            SqlStatement::Rollback { .. } => todo!(),
            SqlStatement::CreateSchema {
                schema_name,
                if_not_exists,
            } => todo!(),
            SqlStatement::Explain {
                describe_alias,
                analyze,
                verbose,
                statement,
                format,
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
