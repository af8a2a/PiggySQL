use crate::execution::executor::{BoxedExecutor, DatabaseError};
use crate::planner::operator::alter_table::{AddColumnOperator, DropColumnOperator};

use crate::types::tuple_builder::TupleBuilder;
use crate::types::value::DataValue;

use std::sync::Arc;

use crate::types::index::Index;
use crate::{execution::executor::Executor, storage::Transaction};

pub struct AddColumn {
    op: AddColumnOperator,
    input: BoxedExecutor,
}

impl From<(AddColumnOperator, BoxedExecutor)> for AddColumn {
    fn from((op, input): (AddColumnOperator, BoxedExecutor)) -> Self {
        Self { op, input }
    }
}

impl<T: Transaction> Executor<T> for AddColumn {
    fn execute(self, transaction: &mut T) -> BoxedExecutor {
        let AddColumnOperator {
            table_name,
            column,
            if_not_exists,
        } = &self.op;
        let mut unique_values = column.desc().is_unique.then(|| Vec::new());

        for tuple in self.input?.iter_mut() {
            tuple.columns.push(Arc::new(column.clone()));
            if let Some(value) = column.default_value() {
                if let Some(unique_values) = &mut unique_values {
                    unique_values.push((tuple.id.clone().unwrap(), value.clone()));
                }
                tuple.values.push(value);
            } else {
                tuple.values.push(Arc::new(DataValue::Null));
            }
            transaction.append(table_name, tuple.clone(), true)?;
        }
        let col_id = transaction.add_column(table_name, column, *if_not_exists)?;

        // Unique Index
        let table = transaction
            .table(table_name.clone())
            .expect("table fetch error");
        let unique_meta = table.get_unique_index(&col_id).cloned();
        if let (Some(unique_values), Some(unique_meta)) = (unique_values, unique_meta) {
            for (tuple_id, value) in unique_values {
                let index = Index {
                    id: unique_meta.id,
                    column_values: vec![value],
                };
                transaction.add_index(&table_name, index, vec![tuple_id], true)?;
            }
        }

        let tuple_builder = TupleBuilder::new_result();
        let tuple = tuple_builder.push_result("ALTER TABLE SUCCESS", "1")?;
        Ok(vec![tuple])
    }
}

pub struct DropColumn {
    op: DropColumnOperator,
    input: BoxedExecutor,
}

impl From<(DropColumnOperator, BoxedExecutor)> for DropColumn {
    fn from((op, input): (DropColumnOperator, BoxedExecutor)) -> Self {
        Self { op, input }
    }
}

impl<T: Transaction> Executor<T> for DropColumn {
    fn execute(self, transaction: &mut T) -> BoxedExecutor {
        let DropColumnOperator {
            table_name,
            column_name,
            if_exists,
        } = &self.op;
        let mut option_column_index = None;

        for tuple in self.input?.iter_mut() {
            if option_column_index.is_none() {
                if let Some((column_index, is_primary)) = tuple
                    .columns
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

            let _ = tuple.columns.remove(column_index);
            let _ = tuple.values.remove(column_index);

            transaction.append(table_name, tuple.clone(), true)?;
        }
        transaction.drop_column(table_name, column_name, *if_exists)?;

        let tuple_builder = TupleBuilder::new_result();
        let tuple = tuple_builder.push_result("ALTER TABLE SUCCESS", "1")?;
        Ok(vec![tuple])
    }
}
