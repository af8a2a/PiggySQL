use crate::execution::executor::{build, DatabaseError, Source};
use crate::planner::operator::alter_table::{AddColumnOperator, DropColumnOperator};

use crate::planner::LogicalPlan;
use crate::types::tuple_builder::TupleBuilder;
use crate::types::value::DataValue;

use std::sync::Arc;

use crate::types::index::Index;
use crate::{execution::executor::Executor, storage::Transaction};

pub struct AddColumn {
    op: AddColumnOperator,
    input: LogicalPlan,
}

impl From<(AddColumnOperator, LogicalPlan)> for AddColumn {
    fn from((op, input): (AddColumnOperator, LogicalPlan)) -> Self {
        Self { op, input }
    }
}

impl<T: Transaction> Executor<T> for AddColumn {
    fn execute(self, transaction: &mut T) -> Source {
        let AddColumn { op, input } = self;
        let mut input = build(input, transaction)?;
        let mut unique_values = op.column.desc().is_unique.then(|| Vec::new());

        for tuple in input.iter_mut() {
            // tuple.columns.push(Arc::new(column.clone()));
            if let Some(value) = op.column.default_value() {
                if let Some(unique_values) = &mut unique_values {
                    unique_values.push((tuple.id.clone().unwrap(), value.clone()));
                }
                tuple.values.push(value);
            } else {
                tuple.values.push(Arc::new(DataValue::Null));
            }
            transaction.append(&op.table_name, tuple.clone(), true)?;
        }
        let col_id = transaction.add_column(&op.table_name, &op.column, op.if_not_exists)?;

        // Unique Index
        let table = transaction
            .table(op.table_name.clone())
            .expect("table fetch error");
        let unique_meta = table.get_unique_index(&col_id).cloned();
        if let (Some(unique_values), Some(unique_meta)) = (unique_values, unique_meta) {
            for (tuple_id, value) in unique_values {
                let index = Index {
                    id: unique_meta.id,
                    column_values: vec![value],
                };
                transaction.add_index(&op.table_name, index, vec![tuple_id], true)?;
            }
        }

        let tuple_builder = TupleBuilder::new_result();
        let tuple = tuple_builder.push_result("ALTER TABLE SUCCESS", "1")?;
        Ok(vec![tuple])
    }
}

pub struct DropColumn {
    op: DropColumnOperator,
    input: LogicalPlan,
}

impl From<(DropColumnOperator, LogicalPlan)> for DropColumn {
    fn from((op, input): (DropColumnOperator, LogicalPlan)) -> Self {
        Self { op, input }
    }
}

impl<T: Transaction> Executor<T> for DropColumn {
    fn execute(self, transaction: &mut T) -> Source {
        let DropColumnOperator {
            table_name,
            column_name,
            if_exists,
        } = &self.op;
        let DropColumn { mut input, .. } = self;
        let schema = input.output_schema().clone();
        let mut input = build(input, transaction)?;
        let mut option_column_index = None;

        for tuple in input.iter_mut() {
            if option_column_index.is_none() {
                if let Some((column_index, is_primary)) = schema
                    .iter()
                    .enumerate()
                    .find(|(_, column)| column.name() == column_name)
                    .map(|(i, column)| (i, column.desc.is_primary))
                {
                    if is_primary {
                        Err(DatabaseError::InvalidColumn(
                            "drop of primary key column is not allowed.".to_owned(),
                        ))?;
                    }
                    option_column_index = Some(column_index);
                }
            }
            if option_column_index.is_none() && *if_exists {
                return Ok(vec![]);
            }
            let column_index = option_column_index
                .ok_or_else(|| DatabaseError::InvalidColumn("not found column".to_string()))?;

            let _ = tuple.values.remove(column_index);

            transaction.append(table_name, tuple.clone(), true)?;
        }
        transaction.drop_column(table_name, column_name, *if_exists)?;

        let tuple_builder = TupleBuilder::new_result();
        let tuple = tuple_builder.push_result("ALTER TABLE SUCCESS", "1")?;
        Ok(vec![tuple])
    }
}
