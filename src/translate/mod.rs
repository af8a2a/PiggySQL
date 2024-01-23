pub mod aggregate_function;
pub mod error;
pub mod query;
pub mod types;
pub mod ddl;
pub mod operator;
pub mod expr;
pub mod ast_literal;

use crate::ast::expr::Expr;

use self::error::TranslateError;
use {
    crate::{ast::Statement, result::Result},
    sqlparser::ast::{
        Assignment as SqlAssignment, Expr as SqlExpr, Ident as SqlIdent,
        ObjectName as SqlObjectName, ObjectType as SqlObjectType, Statement as SqlStatement,
        TableFactor, TableWithJoins,
    },
};


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
