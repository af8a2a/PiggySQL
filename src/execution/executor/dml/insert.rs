use crate::catalog::TableName;
use crate::errors::*;
use crate::execution::executor::{build, Executor, Source};
use crate::planner::operator::insert::InsertOperator;
use crate::planner::LogicalPlan;
use crate::storage::Transaction;
use crate::types::index::Index;
use crate::types::tuple::Tuple;
use crate::types::tuple_builder::TupleBuilder;
use crate::types::value::DataValue;

use std::collections::HashMap;
use std::sync::Arc;

pub struct Insert {
    table_name: TableName,
    input: LogicalPlan,
    is_overwrite: bool,
}

impl From<(InsertOperator, LogicalPlan)> for Insert {
    fn from(
        (
            InsertOperator {
                table_name,
                is_overwrite,
            },
            input,
        ): (InsertOperator, LogicalPlan),
    ) -> Self {
        Insert {
            table_name,
            input,
            is_overwrite,
        }
    }
}

impl<T: Transaction> Executor<T> for Insert {
    fn execute(self, transaction: &mut T) -> Source {
        let Insert {
            table_name,
            mut input,
            is_overwrite,
        } = self;
        let schema = input.output_schema().clone();
        let input = build(input, transaction)?;
        let mut primary_key_index = None;
        let mut unique_values = HashMap::new();
        if let Some(table_catalog) = transaction.table(table_name.clone()) {
            for tuple in input.iter() {
                let Tuple { values, .. } = tuple;
                let mut tuple_map = HashMap::new();
                for (i, value) in values.iter().enumerate() {
                    let col = &schema[i];

                    if let Some(col_id) = col.id() {
                        tuple_map.insert(col_id, value.clone());
                    }
                }
                let primary_col_id = primary_key_index.get_or_insert_with(|| {
                    schema
                        .iter()
                        .find(|col| col.desc.is_primary)
                        .map(|col| col.id().unwrap())
                });
                let all_columns = table_catalog.all_columns_with_id();
                let tuple_id = match primary_col_id{
                    Some(primary_col_id) => tuple_map.get(primary_col_id).cloned(),
                    None => None,
                };

                // tuple_map.get(primary_col_id).cloned().unwrap();
                let mut tuple = Tuple {
                    id: tuple_id.clone(),
                    values: Vec::with_capacity(all_columns.len()),
                };
                for (col_id, col) in all_columns {
                    let value = tuple_map
                        .remove(col_id)
                        .or_else(|| col.default_value())
                        .unwrap_or_else(|| Arc::new(DataValue::none(col.datatype())));

                    if col.desc.is_unique && !value.is_null() {
                        unique_values
                            .entry(col.id())
                            .or_insert_with(Vec::new)
                            .push((tuple_id.clone(), value.clone()))
                    }
                    if value.is_null() && !col.nullable {
                        return Err(DatabaseError::InternalError(format!(
                            "Non-null fields do not allow null values to be passed in: {:?}",
                            col
                        )));
                    }

                    tuple.values.push(value)
                }

                transaction.append(&table_name, tuple, is_overwrite)?;
            }
            // Unique Index
            for (col_id, values) in unique_values {
                if let Some(index_meta) = table_catalog.get_unique_index(&col_id.unwrap()) {
                    for (tuple_id, value) in values {
                        let index = Index {
                            id: index_meta.id,
                            column_values: vec![value],
                        };

                        transaction.add_index(&table_name, index, vec![tuple_id.unwrap()], true)?;
                    }
                }
            }
        }
        let tuple_builder = TupleBuilder::new_result();
        let tuple = tuple_builder.push_result("INSERT SUCCESS", &format!("{}", input.len()))?;
        Ok(vec![tuple])
    }
}
