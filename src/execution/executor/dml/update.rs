use crate::catalog::{ColumnCatalog, TableName};
use crate::execution::executor::{BoxedExecutor, Executor};
use crate::errors::*;
use crate::expression::ScalarExpression;
use crate::planner::operator::update::UpdateOperator;
use crate::storage::Transaction;
use crate::types::index::Index;

use std::collections::HashSet;
use std::sync::Arc;

pub struct Update {
    table_name: TableName,
    input: BoxedExecutor, //select source for update
    columns: Vec<Arc<ColumnCatalog>>,
    set_expr: Vec<ScalarExpression>,
}

impl From<(UpdateOperator, BoxedExecutor)> for Update {
    fn from(
        (
            UpdateOperator {
                columns,
                set_expr,
                table_name,
            },
            input,
        ): (UpdateOperator, BoxedExecutor),
    ) -> Self {
        Update {
            table_name,
            input,
            columns,
            set_expr,
        }
    }
}

impl<T: Transaction> Executor<T> for Update {
    fn execute(self, transaction: &mut T) -> BoxedExecutor {
        let Update {
            table_name,
            input,
            columns,
            set_expr,
        } = self;

        if let Some(table_catalog) = transaction.table(table_name.clone()) {
            //避免halloween问题
            let mut update_col = HashSet::new();

            for col in columns.iter() {
                update_col.insert(col.id());
            }
            //Seqscan遍历元组
            for tuple in input?.iter_mut() {
                // eprintln!("tuple:{}", tuple);
                let is_overwrite = true;

                for (i,column) in tuple
                    .columns
                    .iter()
                    .filter(|col| update_col.contains(&col.id()))
                    .enumerate()
                {
                    let value = set_expr[i].eval(tuple)?;

                    if column.desc.is_primary {
                        //refuse to update primary key
                        return Err(DatabaseError::InternalError("Update Primary key".into()));
                    }
                    //更新索引
                    if column.desc.is_unique && value != tuple.values[column.id().unwrap() as usize] {
                        if let Some(index_meta) =
                            table_catalog.get_unique_index(&column.id().unwrap())
                        {
                            let mut index = Index {
                                id: index_meta.id,
                                column_values: vec![tuple.values[i].clone()],
                            };
                            transaction.del_index(&table_name, &index)?;

                            if !value.is_null() {
                                index.column_values[0] = value.clone();
                                transaction.add_index(
                                    &table_name,
                                    index,
                                    vec![tuple.id.clone().unwrap()],
                                    true,
                                )?;
                            }
                        }
                    }
                    transaction.delete(&table_name, tuple.id.clone().unwrap())?;
                    tuple.values[column.id().unwrap() as usize] = value.clone();
                }
                transaction.append(&table_name, tuple.clone(), is_overwrite)?;
            }
        }
        Ok(vec![])
    }
}
