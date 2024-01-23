use crate::ast::{
    ast_literal::AstLiteral,
    expr::Expr,
    operator::{BinaryOperator, UnaryOperator},
    query::{OrderByExpr, Query},
};

use super::{aggregate_function::translate_function, error::TranslateError};

use {
    crate::{ast::Statement, result::Result},
    sqlparser::ast::{Expr as SqlExpr, OrderByExpr as SqlOrderByExpr},
};

impl Expr {
    pub fn from(expr: &SqlExpr) -> Result<Self> {
        match expr {
            SqlExpr::Identifier(id) => Ok(Expr::Identifier(id.value.clone())),
            SqlExpr::CompoundIdentifier(idents) => (idents.len() == 2)
                .then(|| Expr::CompoundIdentifier {
                    alias: idents[0].value.clone(),
                    ident: idents[1].value.clone(),
                })
                .ok_or_else(|| {
                    TranslateError::UnsupportedExpr(format!(
                        "{}.{}",
                        idents[0].value, idents[1].value
                    ))
                    .into()
                }),
            SqlExpr::IsNull(expr) => Expr::from(expr).map(Box::new).map(Expr::IsNull),
            SqlExpr::IsNotNull(expr) => Expr::from(expr).map(Box::new).map(Expr::IsNotNull),
            SqlExpr::InList {
                expr,
                list,
                negated,
            } => Ok(Expr::InList {
                expr: Expr::from(expr).map(Box::new)?,
                list: list.iter().map(Expr::from).collect::<Result<_>>()?,
                negated: *negated,
            }),
            SqlExpr::InSubquery {
                expr,
                subquery,
                negated,
            } => Ok(Expr::InSubquery {
                expr: Expr::from(expr).map(Box::new)?,
                subquery: Query::from(subquery).map(Box::new)?,
                negated: *negated,
            }),
            SqlExpr::Between {
                expr,
                negated,
                low,
                high,
            } => Ok(Expr::Between {
                expr: Expr::from(expr).map(Box::new)?,
                negated: *negated,
                low: Expr::from(low).map(Box::new)?,
                high: Expr::from(high).map(Box::new)?,
            }),
            SqlExpr::BinaryOp { left, op, right } => Ok(Expr::BinaryOp {
                left: Expr::from(left).map(Box::new)?,
                op: BinaryOperator::from(op)?,
                right: Expr::from(right).map(Box::new)?,
            }),
            SqlExpr::Like {
                negated,
                expr,
                pattern,
                escape_char: None,
            } => Ok(Expr::Like {
                expr: Expr::from(expr).map(Box::new)?,
                negated: *negated,
                pattern: Expr::from(pattern).map(Box::new)?,
            }),
            SqlExpr::UnaryOp { op, expr } => Ok(Expr::UnaryOp {
                op: UnaryOperator::from(op)?,
                expr: Expr::from(expr).map(Box::new)?,
            }),
            SqlExpr::Nested(expr) => Expr::from(expr).map(Box::new).map(Expr::Nested),
            SqlExpr::Value(value) => AstLiteral::from(value).map(Expr::Literal),
            //todo
            SqlExpr::Function(function) => translate_function(function),
            SqlExpr::Exists { subquery, negated } => Ok(Expr::Exists {
                subquery: Query::from(subquery).map(Box::new)?,
                negated: *negated,
            }),
            SqlExpr::Subquery(subquery) => Query::from(subquery).map(Box::new).map(Expr::Subquery),
            _ => unimplemented!(),
        }
    }
}

impl OrderByExpr {
    pub fn from(sql_order_by_expr: &SqlOrderByExpr) -> Result<OrderByExpr> {
        let SqlOrderByExpr {
            expr,
            asc,
            nulls_first,
        } = sql_order_by_expr;

        if nulls_first.is_some() {
            return Err(TranslateError::OrderByNullsFirstOrLastNotSupported.into());
        }

        Ok(OrderByExpr {
            expr: Expr::from(expr)?,
            asc: *asc,
        })
    }
}
