use crate::catalog::{ColumnCatalog, ColumnRef, Schema, SchemaRef};
use crate::errors::DatabaseError;
use crate::errors::*;
use crate::execution::executor::dql::projection::Projection;
use crate::execution::executor::{build, Executor, Source};
use crate::expression::ScalarExpression;
use crate::planner::operator::join::{JoinCondition, JoinOperator, JoinType};
use crate::planner::LogicalPlan;
use crate::storage::Transaction;
use crate::types::tuple::Tuple;
use crate::types::value::{DataValue, NULL_VALUE};
use itertools::Itertools;
use std::sync::Arc;

use super::joins_nullable;

/// Equivalent condition
struct EqualCondition {
    on_left_keys: Vec<ScalarExpression>,
    on_right_keys: Vec<ScalarExpression>,
    left_schema: SchemaRef,
    right_schema: SchemaRef,
}

impl EqualCondition {
    /// Constructs a new `EqualCondition`
    /// If the `on_left_keys` and `on_right_keys` are empty, it means no equivalent condition
    /// Note: `on_left_keys` and `on_right_keys` are either all empty or none of them.
    fn new(
        on_left_keys: Vec<ScalarExpression>,
        on_right_keys: Vec<ScalarExpression>,
        left_schema: Arc<Schema>,
        right_schema: Arc<Schema>,
    ) -> EqualCondition {
        if !on_left_keys.is_empty() && on_left_keys.len() != on_right_keys.len() {
            unreachable!("Unexpected join on condition.")
        }
        EqualCondition {
            on_left_keys,
            on_right_keys,
            left_schema,
            right_schema,
        }
    }

    /// Compare left tuple and right tuple on equivalent condition
    /// `left_tuple` must be from the [`NestedLoopJoin::left_input`]
    /// `right_tuple` must be from the [`NestedLoopJoin::right_input`]
    fn equals(&self, left_tuple: &Tuple, right_tuple: &Tuple) -> Result<bool> {
        if self.on_left_keys.is_empty() {
            return Ok(true);
        }
        let left_values =
            Projection::projection(left_tuple, &self.on_left_keys, &self.left_schema)?;
        let right_values =
            Projection::projection(right_tuple, &self.on_right_keys, &self.right_schema)?;

        Ok(left_values == right_values)
    }
}

/// NestedLoopJoin using nested loop join algorithm to execute a join operation.
/// One input will be selected to be the inner table and the other will be the outer
/// | JoinType                       |  Inner-table   |   Outer-table  |  
/// |--------------------------------|----------------|----------------|  
/// | Inner/Left                     |    right       |      left      |  
/// |--------------------------------|----------------|----------------|  
/// | Right                          |    left        |      right     |  
/// |--------------------------------|----------------|----------------|  
/// | Full                           |  not supported |  not supported |  
pub struct NestedLoopJoin {
    left_input: LogicalPlan,
    right_input: LogicalPlan,
    output_schema_ref: SchemaRef,
    ty: JoinType,
    filter: Option<ScalarExpression>,
    eq_cond: EqualCondition,
}

impl From<(JoinOperator, LogicalPlan, LogicalPlan)> for NestedLoopJoin {
    fn from(
        (JoinOperator { on, join_type, .. }, left_input, right_input): (
            JoinOperator,
            LogicalPlan,
            LogicalPlan,
        ),
    ) -> Self {
        let ((mut on_left_keys, mut on_right_keys), filter) = match on {
            JoinCondition::On { on, filter } => (on.into_iter().unzip(), filter),
            JoinCondition::None => ((vec![], vec![]), None),
        };

        let (mut left_input, mut right_input) = (left_input, right_input);
        let mut left_schema = left_input.output_schema().clone();
        let mut right_schema = right_input.output_schema().clone();
        let output_schema_ref = Self::merge_schema(&left_schema, &right_schema, join_type);

        if matches!(join_type, JoinType::Right) {
            std::mem::swap(&mut left_input, &mut right_input);
            std::mem::swap(&mut on_left_keys, &mut on_right_keys);
            std::mem::swap(&mut left_schema, &mut right_schema);
        }

        let eq_cond = EqualCondition::new(
            on_left_keys,
            on_right_keys,
            left_schema.clone(),
            right_schema.clone(),
        );

        NestedLoopJoin {
            ty: join_type,
            left_input,
            right_input,
            output_schema_ref,
            filter,
            eq_cond,
        }
    }
}

impl<T: Transaction> Executor<T> for NestedLoopJoin {
    fn execute(self, transaction: &mut T) -> Source {
        let NestedLoopJoin {
            ty,
            left_input,
            right_input,
            output_schema_ref,
            filter,
            eq_cond,
            ..
        } = self;
        if matches!(self.ty, JoinType::Full) {
            unreachable!("{} cannot be handled in nested loop join", self.ty)
        }
        let mut tuples = vec![];
        let right_schema_len = eq_cond.right_schema.len();
        let left_tuples = build(left_input, transaction)?;
        let right_tuples = build(right_input.clone(), transaction)?;

        for left_tuple in left_tuples {
            let mut has_matched = false;

            for right_tuple in right_tuples.iter().cloned() {
                let tuple = match (filter.as_ref(), eq_cond.equals(&left_tuple, &right_tuple)?) {
                    (None, true) if matches!(ty, JoinType::Right) => {
                        Self::emit_tuple(&right_tuple, &left_tuple, ty, true)
                    }
                    (None, true) => Self::emit_tuple(&left_tuple, &right_tuple, ty, true),
                    (Some(filter), true) => {
                        let new_tuple = Self::merge_tuple(&left_tuple, &right_tuple, &ty);
                        let value = filter.eval(&new_tuple, &output_schema_ref)?;
                        match value.as_ref() {
                            DataValue::Boolean(Some(true)) => {
                                let tuple = match ty {
                                    JoinType::Right => {
                                        Self::emit_tuple(&right_tuple, &left_tuple, ty, true)
                                    }
                                    _ => Self::emit_tuple(&left_tuple, &right_tuple, ty, true),
                                };
                                has_matched = true;
                                tuple
                            }
                            DataValue::Boolean(Some(_) | None) => None,
                            _ => return Err(DatabaseError::InvalidType),
                        }
                    }
                    _ => None,
                };

                if let Some(tuple) = tuple {
                    tuples.push(tuple);
                }

            }

            // handle no matched tuple case
            let tuple = match ty {
                JoinType::Left | JoinType::Right if !has_matched => {
                    let right_tuple = Tuple {
                        id: None,
                        values: vec![NULL_VALUE.clone(); right_schema_len],
                    };
                    if matches!(ty, JoinType::Right) {
                        Self::emit_tuple(&right_tuple, &left_tuple, ty, false)
                    } else {
                        Self::emit_tuple(&left_tuple, &right_tuple, ty, false)
                    }
                }
                _ => None,
            };
            if let Some(tuple) = tuple {
                tuples.push(tuple);
            }
        }
        tuples.iter().for_each(|tuple|println!("tuple: {}", tuple));
        Ok(tuples)
    }
}

impl NestedLoopJoin {
    /// Emit a tuple according to the join type.
    ///
    /// `left_tuple`: left tuple to be included.
    /// `right_tuple` right tuple to be included.
    /// `ty`: the type of join
    /// `is_match`: whether [`NestedLoopJoin::left_input`] and [`NestedLoopJoin::right_input`] are matched
    fn emit_tuple(
        left_tuple: &Tuple,
        right_tuple: &Tuple,
        ty: JoinType,
        is_matched: bool,
    ) -> Option<Tuple> {
        let left_len = left_tuple.values.len();
        let mut values = left_tuple
            .values
            .iter()
            .cloned()
            .chain(right_tuple.values.clone())
            .collect_vec();
        match ty {
            JoinType::Inner | JoinType::Cross if !is_matched => values.clear(),
            JoinType::Left if !is_matched => {
                values
                    .iter_mut()
                    .skip(left_len)
                    .for_each(|v| *v = NULL_VALUE.clone());
            }
            JoinType::Right if !is_matched => {
                (0..left_len).for_each(|i| {
                    values[i] = NULL_VALUE.clone();
                });
            }
            JoinType::Full => todo!("Not support now."),
            _ => (),
        };

        if values.is_empty() {
            return None;
        }

        Some(Tuple { id: None, values })
    }

    /// Merge the two tuples.
    /// `left_tuple` must be from the `NestedLoopJoin.left_input`
    /// `right_tuple` must be from the `NestedLoopJoin.right_input`
    fn merge_tuple(left_tuple: &Tuple, right_tuple: &Tuple, ty: &JoinType) -> Tuple {
        match ty {
            JoinType::Right => Tuple {
                id: None,
                values: right_tuple
                    .values
                    .iter()
                    .cloned()
                    .chain(left_tuple.clone().values)
                    .collect_vec(),
            },
            _ => Tuple {
                id: None,
                values: left_tuple
                    .values
                    .iter()
                    .cloned()
                    .chain(right_tuple.clone().values)
                    .collect_vec(),
            },
        }
    }

    fn merge_schema(
        left_schema: &[ColumnRef],
        right_schema: &[ColumnRef],
        ty: JoinType,
    ) -> Arc<Vec<ColumnRef>> {
        let (left_force_nullable, right_force_nullable) = joins_nullable(&ty);

        let mut join_schema = vec![];
        for column in left_schema.iter() {
            let mut temp = ColumnCatalog::clone(column);
            temp.nullable = left_force_nullable;
            join_schema.push(Arc::new(temp));
        }
        for column in right_schema.iter() {
            let mut temp = ColumnCatalog::clone(column);
            temp.nullable = right_force_nullable;
            join_schema.push(Arc::new(temp));
        }
        Arc::new(join_schema)
    }
}
