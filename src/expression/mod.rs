pub mod agg;

use serde::{Deserialize, Serialize};
use sqlparser::ast::DataType;

use crate::{
    store::schema::{Column, ColumnRef},
    types::{DataValue, ValueRef},
};

use self::agg::Aggregate;

/// ScalarExpression represnet all scalar expression in SQL.
/// SELECT a+1, b FROM t1.
/// a+1 -> ScalarExpression::Unary(a + 1)
/// b   -> ScalarExpression::ColumnRef()
#[derive(Debug, PartialEq, Eq, Clone, Hash, Serialize, Deserialize)]
pub enum ScalarExpression {
    Constant(ValueRef),
    ColumnRef(ColumnRef),
    Alias {
        expr: Box<ScalarExpression>,
        alias: String,
    },
    IsNull {
        negated: bool,
        expr: Box<ScalarExpression>,
    },
    Unary {
        op: UnaryOperator,
        expr: Box<ScalarExpression>,
        ty: DataType,
    },
    Binary {
        op: BinaryOperator,
        left_expr: Box<ScalarExpression>,
        right_expr: Box<ScalarExpression>,
        ty: DataType,
    },
    AggCall {
        distinct: bool,
        kind: Aggregate,
        args: Vec<ScalarExpression>,
        ty: DataType,
    },
    In {
        negated: bool,
        expr: Box<ScalarExpression>,
        args: Vec<ScalarExpression>,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum UnaryOperator {
    Plus,
    Minus,
    Not,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum BinaryOperator {
    Plus,
    Minus,
    Multiply,
    Divide,

    Modulo,

    Gt,
    Lt,
    GtEq,
    LtEq,
    Eq,
    NotEq,
    Like,

    And,
    Or,
    Xor,
}
