use crate::{ast::aggregate_function::{Aggregate, CountArgExpr}, result::Result};
use super::{error::TranslateError, translate_object_name};

use {
    crate::ast::expr::Expr,
    sqlparser::ast::{
        Expr as SqlExpr, Function as SqlFunction, FunctionArg as SqlFunctionArg,
        FunctionArgExpr as SqlFunctionArgExpr,
    },
};

pub fn translate_function(sql_function: &SqlFunction) -> Result<Expr> {
    let SqlFunction { name, args, .. } = sql_function;
    let name = translate_object_name(name)?.to_uppercase();

    let function_arg_exprs = args
        .iter()
        .map(|arg| match arg {
            SqlFunctionArg::Named { .. } => {
                Err(TranslateError::NamedFunctionArgNotSupported.into())
            }
            SqlFunctionArg::Unnamed(arg_expr) => Ok(arg_expr),
        })
        .collect::<Result<Vec<_>>>()?;

    if name.as_str() == "COUNT" {
        check_len(name, args.len(), 1)?;

        let count_arg = match function_arg_exprs[0] {
            SqlFunctionArgExpr::Expr(expr) => CountArgExpr::Expr(Expr::from(expr)?),
            SqlFunctionArgExpr::QualifiedWildcard(idents) => {
                let table_name = translate_object_name(idents)?;
                let idents = format!("{}.*", table_name);

                return Err(TranslateError::QualifiedWildcardInCountNotSupported(idents).into());
            }
            SqlFunctionArgExpr::Wildcard => CountArgExpr::Wildcard,
        };

        return Ok(Expr::Aggregate(Box::new(Aggregate::Count(count_arg))));
    }

    let args = translate_function_arg_exprs(function_arg_exprs)?;

    match name.as_str() {
        "SUM" => translate_aggregate_one_arg(Aggregate::Sum, args, name),
        "MIN" => translate_aggregate_one_arg(Aggregate::Min, args, name),
        "MAX" => translate_aggregate_one_arg(Aggregate::Max, args, name),
        "AVG" => translate_aggregate_one_arg(Aggregate::Avg, args, name),
        "VARIANCE" => translate_aggregate_one_arg(Aggregate::Variance, args, name),
        "STDEV" => translate_aggregate_one_arg(Aggregate::Stdev, args, name),
        _ => unimplemented!(),
    }
}

fn check_len(name: String, found: usize, expected: usize) -> Result<()> {
    if found == expected {
        Ok(())
    } else {
        Err(TranslateError::FunctionArgsLengthNotMatching {
            name,
            found,
            expected,
        }
        .into())
    }
}


fn translate_aggregate_one_arg<T: FnOnce(Expr) -> Aggregate>(
    func: T,
    args: Vec<&SqlExpr>,
    name: String,
) -> Result<Expr> {
    check_len(name, args.len(), 1)?;

    Expr::from(args[0])
        .map(func)
        .map(Box::new)
        .map(Expr::Aggregate)
}


pub fn translate_function_arg_exprs(
    function_arg_exprs: Vec<&SqlFunctionArgExpr>,
) -> Result<Vec<&SqlExpr>> {
    function_arg_exprs
        .into_iter()
        .map(|function_arg| match function_arg {
            SqlFunctionArgExpr::Expr(expr) => Ok(expr),
            SqlFunctionArgExpr::Wildcard | SqlFunctionArgExpr::QualifiedWildcard(_) => {
                Err(TranslateError::WildcardFunctionArgNotAccepted.into())
            }
        })
        .collect::<Result<Vec<_>>>()
}
