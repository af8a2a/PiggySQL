pub mod error;
use self::error::TranslateError;
use crate::error::Result;
use sqlparser::ast::Expr as SqlExpr;

use super::expression::Expr;

impl Expr {
    pub fn from(expr: SqlExpr) -> Result<Self> {
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
            SqlExpr::IsNull(expr) => Expr::from(*expr).map(Box::new).map(Expr::IsNull),
            SqlExpr::IsNotNull(expr) => Expr::from(*expr).map(Box::new).map(Expr::IsNotNull),
            SqlExpr::InList {
                expr,
                list,
                negated,
            } => todo!(),
            SqlExpr::InSubquery {
                expr,
                subquery,
                negated,
            } => todo!(),
            SqlExpr::InUnnest {
                expr,
                array_expr,
                negated,
            } => todo!(),
            SqlExpr::Between {
                expr,
                negated,
                low,
                high,
            } => todo!(),
            SqlExpr::BinaryOp { left, op, right } => todo!(),
            SqlExpr::Like {
                negated,
                expr,
                pattern,
                escape_char,
            } => todo!(),
            SqlExpr::ILike {
                negated,
                expr,
                pattern,
                escape_char,
            } => todo!(),
            SqlExpr::SimilarTo {
                negated,
                expr,
                pattern,
                escape_char,
            } => todo!(),
            SqlExpr::RLike {
                negated,
                expr,
                pattern,
                regexp,
            } => todo!(),
            SqlExpr::AnyOp {
                left,
                compare_op,
                right,
            } => todo!(),
            SqlExpr::AllOp {
                left,
                compare_op,
                right,
            } => todo!(),
            SqlExpr::UnaryOp { op, expr } => todo!(),
            SqlExpr::Convert {
                expr,
                data_type,
                charset,
                target_before_value,
            } => todo!(),
            SqlExpr::Cast {
                expr,
                data_type,
                format,
            } => todo!(),
            SqlExpr::TryCast {
                expr,
                data_type,
                format,
            } => todo!(),
            SqlExpr::SafeCast {
                expr,
                data_type,
                format,
            } => todo!(),
            SqlExpr::AtTimeZone {
                timestamp,
                time_zone,
            } => todo!(),
            SqlExpr::Extract { field, expr } => todo!(),
            SqlExpr::Ceil { expr, field } => todo!(),
            SqlExpr::Floor { expr, field } => todo!(),
            SqlExpr::Position { expr, r#in } => todo!(),
            SqlExpr::Substring {
                expr,
                substring_from,
                substring_for,
                special,
            } => todo!(),
            SqlExpr::Trim {
                expr,
                trim_where,
                trim_what,
                trim_characters,
            } => todo!(),
            SqlExpr::Overlay {
                expr,
                overlay_what,
                overlay_from,
                overlay_for,
            } => todo!(),
            SqlExpr::Collate { expr, collation } => todo!(),
            SqlExpr::Nested(_) => todo!(),
            SqlExpr::Value(_) => todo!(),
            SqlExpr::IntroducedString { introducer, value } => todo!(),
            SqlExpr::TypedString { data_type, value } => todo!(),
            SqlExpr::MapAccess { column, keys } => todo!(),
            SqlExpr::Function(_) => todo!(),
            SqlExpr::AggregateExpressionWithFilter { expr, filter } => todo!(),
            SqlExpr::Case {
                operand,
                conditions,
                results,
                else_result,
            } => todo!(),
            SqlExpr::Exists { subquery, negated } => todo!(),
            SqlExpr::Subquery(_) => todo!(),
            SqlExpr::ArraySubquery(_) => todo!(),
            SqlExpr::ListAgg(_) => todo!(),
            SqlExpr::ArrayAgg(_) => todo!(),
            SqlExpr::GroupingSets(_) => todo!(),
            SqlExpr::Cube(_) => todo!(),
            SqlExpr::Rollup(_) => todo!(),
            SqlExpr::Tuple(_) => todo!(),
            SqlExpr::Struct { values, fields } => todo!(),
            SqlExpr::Named { expr, name } => todo!(),
            SqlExpr::ArrayIndex { obj, indexes } => todo!(),
            SqlExpr::Array(_) => todo!(),
            SqlExpr::Interval(_) => todo!(),
            _ => unimplemented!(),
        }
    }
}
