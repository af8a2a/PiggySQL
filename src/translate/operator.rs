use {
    super::TranslateError,
    crate::{
        ast::operator::{BinaryOperator, UnaryOperator},
        result::Result,
    },
    sqlparser::ast::{BinaryOperator as SqlBinaryOperator, UnaryOperator as SqlUnaryOperator},
};

impl UnaryOperator {
    pub fn from(sql_unary_operator: &SqlUnaryOperator) -> Result<UnaryOperator> {
        match sql_unary_operator {
            SqlUnaryOperator::Plus => Ok(UnaryOperator::Plus),
            SqlUnaryOperator::Minus => Ok(UnaryOperator::Minus),
            SqlUnaryOperator::Not => Ok(UnaryOperator::Not),
            _ => {
                Err(TranslateError::UnreachableUnaryOperator(sql_unary_operator.to_string()).into())
            }
        }
    }
}

impl BinaryOperator {
    pub fn from(sql_binary_operator: &SqlBinaryOperator) -> Result<BinaryOperator> {
        match sql_binary_operator {
            SqlBinaryOperator::Plus => Ok(BinaryOperator::Plus),
            SqlBinaryOperator::Minus => Ok(BinaryOperator::Minus),
            SqlBinaryOperator::Multiply => Ok(BinaryOperator::Multiply),
            SqlBinaryOperator::Divide => Ok(BinaryOperator::Divide),
            SqlBinaryOperator::Modulo => Ok(BinaryOperator::Modulo),
            SqlBinaryOperator::Gt => Ok(BinaryOperator::Gt),
            SqlBinaryOperator::Lt => Ok(BinaryOperator::Lt),
            SqlBinaryOperator::GtEq => Ok(BinaryOperator::GtEq),
            SqlBinaryOperator::LtEq => Ok(BinaryOperator::LtEq),
            SqlBinaryOperator::Eq => Ok(BinaryOperator::Eq),
            SqlBinaryOperator::NotEq => Ok(BinaryOperator::NotEq),
            SqlBinaryOperator::And => Ok(BinaryOperator::And),
            SqlBinaryOperator::Or => Ok(BinaryOperator::Or),
            _ => Err(
                TranslateError::UnsupportedBinaryOperator(sql_binary_operator.to_string()).into(),
            ),
        }
    }
}
