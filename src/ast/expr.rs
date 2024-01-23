use crate::error::*;
use serde::{Deserialize, Serialize};
use sqlparser::ast::Query;

use super::{
    ast_literal::AstLiteral,
    operator::{BinaryOperator, UnaryOperator},
    types::Value,
};

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Expr {
    Identifier(String),
    CompoundIdentifier {
        alias: String,
        ident: String,
    },
    Literal(AstLiteral),

    IsNull(Box<Expr>),
    IsNotNull(Box<Expr>),
    InList {
        expr: Box<Expr>,
        list: Vec<Expr>,
        negated: bool,
    },
    InSubquery {
        expr: Box<Expr>,
        subquery: Box<Query>,
        negated: bool,
    },
    Between {
        expr: Box<Expr>,
        negated: bool,
        low: Box<Expr>,
        high: Box<Expr>,
    },

    BinaryOp {
        left: Box<Expr>,
        op: BinaryOperator,
        right: Box<Expr>,
    },
    UnaryOp {
        op: UnaryOperator,
        expr: Box<Expr>,
    },
}

impl Expr {
    pub fn eval(&mut self) -> Result<Value> {
        match self {
            Expr::Identifier(id) => Ok(Value::String(id.clone())),
            Expr::CompoundIdentifier { alias, ident } => todo!(),
            Expr::IsNull(_) => todo!(),
            Expr::IsNotNull(_) => todo!(),
            Expr::InList {
                expr,
                list,
                negated,
            } => todo!(),
            Expr::InSubquery {
                expr,
                subquery,
                negated,
            } => todo!(),
            Expr::Between {
                expr,
                negated,
                low,
                high,
            } => todo!(),
            Expr::BinaryOp { left, op, right } => todo!(),
            Expr::UnaryOp { op, expr } => {
                todo!()
            }
            _ => todo!(),
        }
    }
}
