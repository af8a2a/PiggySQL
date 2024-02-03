use crate::catalog::TableName;
use crate::execution::executor::{BoxedExecutor, Executor};
use crate::execution::ExecutorError;
use crate::expression::ScalarExpression;
use crate::planner::operator::update::UpdateOperator;
use crate::storage::Transaction;
use crate::types::index::Index;
use crate::types::tuple::Tuple;
use std::cell::RefCell;
use std::collections::{HashMap, HashSet};

pub struct Update {
    table_name: TableName,
    input: BoxedExecutor,  //select source for update
    values: BoxedExecutor, //select source for update
    set_expr: Vec<ScalarExpression>,
}

impl From<(UpdateOperator, BoxedExecutor, BoxedExecutor)> for Update {
    fn from(
        (
            UpdateOperator {
                set_expr,
                table_name,
            },
            input,
            values,
        ): (UpdateOperator, BoxedExecutor, BoxedExecutor),
    ) -> Self {
        Update {
            table_name,
            input,
            values,
            set_expr,
        }
    }
}

impl<T: Transaction> Executor<T> for Update {
    fn execute(self, transaction: &RefCell<T>) -> BoxedExecutor {
        let mut transaction = transaction.borrow_mut();
        let Update {
            table_name,
            input,
            values,
            set_expr,
        } = self;
        if let Some(table_catalog) = transaction.table(table_name.clone()) {
            //避免halloween问题
            let mut unique_set = HashSet::new();
            let mut value_map = HashMap::new();

            for tuple in values?.iter() {
                let Tuple {
                    columns, values, ..
                } = tuple;
                for i in 0..columns.len() {
                    value_map.insert(columns[i].id(), values[i].clone());
                }
            }
            //Seqscan遍历元组
            for tuple in input?.iter_mut() {
                let mut is_overwrite = true;

                for (i, column) in tuple.columns.iter().enumerate() {
                    if !unique_set.contains(&column.id()) && value_map.contains_key(&column.id()) {
                        let value = set_expr[i].eval(tuple)?;
                        unique_set.insert(column.id());
                        if column.desc.is_primary {
                            //refuse to update primary key
                            // let old_key = tuple.id.replace(value.clone()).unwrap();
                            // transaction.delete(&table_name, old_key)?;
                            // is_overwrite = false;
                            return Err(ExecutorError::InternalError("Update Primary key".into()));
                        }
                        //更新索引
                        if column.desc.is_unique && value != tuple.values[i] {
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

                        tuple.values[i] = value.clone();
                    }
                }
                transaction.append(&table_name, tuple.clone(), is_overwrite)?;
            }
        }
        Ok(vec![])
    }
}
