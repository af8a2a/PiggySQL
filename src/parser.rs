use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::ParserError;
use sqlparser::{ast::Statement as SqlStatement, parser::Parser};
const DIALECT: PostgreSqlDialect = PostgreSqlDialect {};

pub fn parse(sql: &str) -> Result<Vec<SqlStatement>, ParserError> {
    Parser::parse_sql(&DIALECT, sql.to_lowercase().as_str())
}

#[cfg(test)]
mod test {
    use super::*;
    #[test]
    fn test_parser() {
        let sql = "select * from test";
        let ast = parse(sql);
        println!("{:#?}", ast);
    }
}
