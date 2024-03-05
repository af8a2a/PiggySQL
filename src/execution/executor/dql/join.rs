use crate::planner::operator::join::JoinType;

use crate::catalog::{ColumnCatalog, ColumnRef, SchemaRef};
use crate::execution::executor::{build, Executor, Source};

use crate::errors::*;
use crate::expression::ScalarExpression;
use crate::planner::operator::join::{JoinCondition, JoinOperator};
use crate::planner::LogicalPlan;
use crate::storage::Transaction;
use crate::types::tuple::Tuple;
use crate::types::value::DataValue;
use ahash::{HashMap, HashMapExt, HashSet, HashSetExt, RandomState};

use itertools::Itertools;
use std::sync::Arc;
///如果分别来自 R 和 S 中的两个 tuples 满足 Join 的条件  
///它们的 Join Attributes 必然相等，那么它们的 Join Attributes 经过某个 hash function 得到的值也必然相等  
///因此 Join 的时候，我们只需要对两个 tables 中 hash 到同样值的 tuples 分别执行 Join 操作即可。  
pub struct HashJoin {
    on: JoinCondition,
    ty: JoinType,
    left_input: LogicalPlan,
    right_input: LogicalPlan,
}

impl From<(JoinOperator, LogicalPlan, LogicalPlan)> for HashJoin {
    fn from(
        (JoinOperator { on, join_type }, left_input, right_input): (
            JoinOperator,
            LogicalPlan,
            LogicalPlan,
        ),
    ) -> Self {
        HashJoin {
            on,
            ty: join_type,
            left_input,
            right_input,
        }
    }
}

impl<T: Transaction> Executor<T> for HashJoin {
    fn execute(self, transaction: &mut T) -> Source {
        let mut tuples = Vec::new();
        let HashJoin {
            on,
            ty,
            mut left_input,
            mut right_input,
        } = self;
        let left_schema = left_input.output_schema().clone();
        let right_schema = right_input.output_schema().clone();
        let left_input = build(left_input, transaction)?;
        let right_input = build(right_input, transaction)?;
        // println!("right_input len: {}", right_input.len());
        // println!("type: {}", ty);
        if ty == JoinType::Cross {
            unreachable!("Cross join should not be in HashJoinExecutor");
        }
        let ((on_left_keys, on_right_keys), filter): (
            (Vec<ScalarExpression>, Vec<ScalarExpression>),
            _,
        ) = match on {
            JoinCondition::On { on, filter } => (on.into_iter().unzip(), filter),
            JoinCondition::None => unreachable!("HashJoin must has on condition"),
        };

        let mut join_columns = Vec::new();
        let mut used_set = HashSet::<u64>::new();
        let mut left_map = HashMap::new();

        let hash_random_state = RandomState::with_seeds(0, 0, 0, 0);
        let (left_force_nullable, right_force_nullable) = joins_nullable(&ty);
        //对Outer Table构造哈希表
        // build phase:
        // 1.construct hashtable, one hash key may contains multiple rows indices.
        // 2.merged all left tuples.
        let mut left_init_flag = false;
        for tuple in left_input {
            let hash = Self::hash_row(&on_left_keys, &left_schema, &hash_random_state, &tuple)?;

            if !left_init_flag {
                Self::columns_filling(&left_schema, &mut join_columns, left_force_nullable);
                left_init_flag = true;
                // for iter in join_columns.iter() {
                //     println!("left join_columns: {}", iter);
                // }
            }

            left_map.entry(hash).or_insert(Vec::new()).push(tuple);
        }
        // println!("left_map: {:#?}", left_map);
        // probe phase
        Self::columns_filling(&right_schema, &mut join_columns, right_force_nullable);
        // for iter in join_columns.iter() {
        //     println!("right join_columns: {}", iter);
        // }

        for tuple in right_input {
            let right_cols_len = right_schema.len();
            //构造hash
            let hash = Self::hash_row(&on_right_keys, &right_schema, &hash_random_state, &tuple)?;
            //构造join columns

            let mut join_tuples = if let Some(tuples) = left_map.get(&hash) {
                let _ = used_set.insert(hash);

                tuples
                    .iter()
                    .map(|Tuple { values, .. }| {
                        let full_values = values
                            .iter()
                            .cloned()
                            .chain(tuple.values.clone())
                            .collect_vec();

                        Tuple {
                            id: None,
                            values: full_values,
                        }
                    })
                    .collect_vec()
            } else if matches!(ty, JoinType::Right | JoinType::Full) {
                let empty_len = join_columns.len() - right_cols_len;
                let values = join_columns[..empty_len]
                    .iter()
                    .map(|col| Arc::new(DataValue::none(col.datatype())))
                    .chain(tuple.values)
                    .collect_vec();

                vec![Tuple { id: None, values }]
            } else {
                vec![]
            };

            // on filter
            if let (Some(expr), false) = (
                &filter,
                join_tuples.is_empty() || matches!(ty, JoinType::Full | JoinType::Cross),
            ) {
                let mut filter_tuples = Vec::with_capacity(join_tuples.len());

                for mut tuple in join_tuples {
                    if let DataValue::Boolean(option) = expr.eval(&tuple, &join_columns)?.as_ref() {
                        if let Some(false) | None = option {
                            let full_cols_len = join_columns.len();
                            let left_cols_len = full_cols_len - right_cols_len;

                            match ty {
                                JoinType::Left => {
                                    for i in left_cols_len..full_cols_len {
                                        let value_type = join_columns[i].datatype();

                                        tuple.values[i] = Arc::new(DataValue::none(value_type))
                                    }
                                    filter_tuples.push(tuple)
                                }
                                JoinType::Right => {
                                    for i in 0..left_cols_len {
                                        let value_type = left_schema[i].datatype();

                                        tuple.values[i] = Arc::new(DataValue::none(value_type))
                                    }
                                    filter_tuples.push(tuple)
                                }
                                _ => (),
                            }
                        } else {
                            filter_tuples.push(tuple)
                        }
                    } else {
                        unreachable!("only bool");
                    }
                }

                join_tuples = filter_tuples;
            }

            for tuple in join_tuples {
                tuples.push(tuple);
            }
        }

        if matches!(ty, JoinType::Left | JoinType::Full) {
            // println!("reach");

            for (hash, tuple) in left_map {
                if used_set.contains(&hash) {
                    continue;
                }

                for Tuple { mut values, .. } in tuple {
                    let mut right_empties = join_columns[left_schema.len()..]
                        .iter()
                        .map(|col| Arc::new(DataValue::none(col.datatype())))
                        .collect_vec();

                    values.append(&mut right_empties);
                    tuples.push(Tuple { id: None, values });
                }
            }
        }
        // for iter in tuples.iter() {
        //     println!("tuples: {}", iter);
        // }
        Ok(tuples)
    }
}

impl HashJoin {
    pub(super) fn columns_filling(
        schema: &SchemaRef,
        join_columns: &mut Vec<ColumnRef>,
        force_nullable: bool,
    ) {
        let mut new_columns = schema
            .iter()
            .cloned()
            .map(|col| {
                let mut new_catalog = ColumnCatalog::clone(&col);
                new_catalog.nullable = force_nullable;

                Arc::new(new_catalog)
            })
            .collect_vec();

        join_columns.append(&mut new_columns);
    }

    pub(super) fn hash_row(
        on_keys: &[ScalarExpression],
        schema: &SchemaRef,
        hash_random_state: &RandomState,
        tuple: &Tuple,
    ) -> Result<u64> {
        let mut values = Vec::with_capacity(on_keys.len());

        for expr in on_keys {
            values.push(expr.eval(tuple, schema)?);
        }

        Ok(hash_random_state.hash_one(values))
    }
}

pub fn joins_nullable(join_type: &JoinType) -> (bool, bool) {
    match join_type {
        JoinType::Inner => (false, false),
        JoinType::Left => (false, true),
        JoinType::Right => (true, false),
        JoinType::Full => (true, true),
        JoinType::Cross => (true, true),
    }
}
