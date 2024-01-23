use {
    super::TranslateError,
    crate::{
        ast::ast_literal::AstLiteral, result::Result
    },
    sqlparser::ast::{
        DateTimeField as SqlDateTimeField, TrimWhereField as SqlTrimWhereField, Value as SqlValue,
    },
};

impl AstLiteral{
    pub fn from(sql_value: &SqlValue) -> Result<AstLiteral> {
        Ok(match sql_value {
            SqlValue::Boolean(v) => AstLiteral::Boolean(*v),
            SqlValue::Number(v, _) => AstLiteral::Number(v.clone()),
            SqlValue::SingleQuotedString(v) => AstLiteral::QuotedString(v.clone()),
            SqlValue::HexStringLiteral(v) => AstLiteral::HexString(v.clone()),
            SqlValue::Null => AstLiteral::Null,
            _ => {
                return Err(TranslateError::UnsupportedAstLiteral(sql_value.to_string()).into());
            }
        })
    }
}