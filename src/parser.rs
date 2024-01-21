use crate::result::{Error, Result};
use sqlparser::ast;
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::{ast::Statement as SqlStatement, parser::Parser};
const DIALECT: PostgreSqlDialect = PostgreSqlDialect {};

pub fn parse(sql: &str) -> Result<Vec<SqlStatement>> {
    Parser::parse_sql(&DIALECT, sql).map_err(|e| Error::Parser(format!("{:#?}", e)))
}
