use crate::catalog::ColumnRef;
use crate::errors::*;
use crate::expression::ScalarExpression;
use crate::types::tuple::Tuple;
use crate::types::value::{DataValue, ValueRef};
use itertools::Itertools;
use lazy_static::lazy_static;
use std::sync::Arc;

lazy_static! {
    static ref NULL_VALUE: ValueRef = Arc::new(DataValue::Null);
}

impl ScalarExpression {
    ///表达式求值  
    ///给定元组，返回表达式的值
    pub fn eval(&self, tuple: &Tuple, schema: &[ColumnRef]) -> Result<ValueRef> {
        // 具名元组的表达式求值
        // if let Some(value) = Self::eval_with_name(tuple, self.output_columns().name(), schema) {
        //     return Ok(value.clone());
        // }
        if let Some(value) = schema
            .iter()
            .find_position(|tul_col| tul_col.summary() == self.output_columns().summary())
            .map(|(i, _)| &tuple.values[i])
        {
            return Ok(value.clone());
        }
        match &self {
            ScalarExpression::Constant(val) => Ok(val.clone()),
            ScalarExpression::ColumnRef(col) => {
                // println!("schema:{:?}", schema);
                // println!("col:{:?}", col.summary());
                let value = schema
                    .iter()
                    .find_position(|tul_col| tul_col.summary() == col.summary())
                    .map(|(i, _)| &tuple.values[i])
                    .unwrap_or(&NULL_VALUE)
                    .clone();
                Ok(value)
            }
            ScalarExpression::Alias { expr, alias } => {
                if let Some(value) = Self::eval_with_name(tuple, alias, schema) {
                    return Ok(value.clone());
                }

                expr.eval(tuple, schema)
            }
            ScalarExpression::TypeCast { expr, ty, .. } => {
                let value = expr.eval(tuple, schema)?;

                Ok(Arc::new(DataValue::clone(&value).cast(ty)?))
            }
            ScalarExpression::Binary {
                left_expr,
                right_expr,
                op,
                ..
            } => {
                let left = left_expr.eval(tuple, schema)?;
                let right = right_expr.eval(tuple, schema)?;

                Ok(Arc::new(DataValue::binary_op(&left, &right, op)?))
            }
            ScalarExpression::IsNull { expr, negated } => {
                let mut is_null = expr.eval(tuple, schema)?.is_null();
                if *negated {
                    is_null = !is_null;
                }
                Ok(Arc::new(DataValue::Boolean(Some(is_null))))
            }
            ScalarExpression::In {
                expr,
                args,
                negated,
            } => {
                let value = expr.eval(tuple, schema)?;
                let mut is_in = false;
                for arg in args {
                    if arg.eval(tuple, schema)? == value {
                        is_in = true;
                        break;
                    }
                }
                if *negated {
                    is_in = !is_in;
                }
                Ok(Arc::new(DataValue::Boolean(Some(is_in))))
            }
            ScalarExpression::Unary { expr, op, .. } => {
                let value = expr.eval(tuple, schema)?;

                Ok(Arc::new(DataValue::unary_op(&value, op)?))
            }
            ScalarExpression::AggCall { .. } => {
                // println!("{}",schema.iter().map(|item|item.to_string()).join(","));
                // schema.iter().for_each(|item|)
                let value = schema
                    .iter()
                    .find_position(|tul_col| tul_col.summary() == self.output_columns().summary())
                    .map(|(i, _)| &tuple.values[i])
                    .unwrap_or(&NULL_VALUE)
                    .clone();
                Ok(value)
            }
        }
    }

    fn eval_with_name<'a>(
        tuple: &'a Tuple,
        name: &str,
        schema: &[ColumnRef],
    ) -> Option<&'a ValueRef> {
        schema
            .iter()
            .find_position(|tul_col| tul_col.name() == name)
            .map(|(i, _)| &tuple.values[i])
    }
}
