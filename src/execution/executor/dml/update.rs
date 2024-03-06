use crate::catalog::{ColumnCatalog, TableName};

use crate::execution::executor::{build, Executor, Source};
use crate::expression::ScalarExpression;
use crate::planner::operator::update::UpdateOperator;
use crate::planner::LogicalPlan;
use crate::storage::Transaction;
use crate::types::index::Index;
use crate::types::tuple_builder::TupleBuilder;

use std::collections::HashSet;
use std::sync::Arc;

pub struct Update {
    table_name: TableName,
    input: LogicalPlan, //select source for update
    columns: Vec<Arc<ColumnCatalog>>,
    set_expr: Vec<ScalarExpression>,
}

impl From<(UpdateOperator, LogicalPlan)> for Update {
    fn from(
        (
            UpdateOperator {
                columns,
                set_expr,
                table_name,
            },
            input,
        ): (UpdateOperator, LogicalPlan),
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
    fn execute(self, transaction: &mut T) -> Source {
        let Update {
            table_name,
            mut input,
            columns,
            set_expr,
        } = self;
        let schema = input.output_schema().clone();
        let input = build(input, transaction)?;
        let  input_len=input.len();

        if let Some(table_catalog) = transaction.table(table_name.clone()) {
            //避免halloween问题
            let mut update_col = HashSet::new();
            let mut update_batch = vec![];
            let mut index_update_batch = vec![];

            for col in columns.iter() {
                update_col.insert(col.id());
            }

            //Seqscan遍历元组
            for tuple in input {
                let mut is_overwrite = true;

                let mut tuple = tuple;
                // eprintln!("tuple:{}", tuple);

                for (i, column) in schema
                    .iter()
                    .filter(|col| update_col.contains(&col.id()))
                    .enumerate()
                {
                    let value = set_expr[i].eval(&tuple,&schema)?;

                    if column.desc.is_primary {
                        //refuse to update primary key
                        let old_key = tuple.id.replace(value.clone()).unwrap();
                        transaction.delete(&table_name, old_key)?;
                        is_overwrite = false;
                        // return Err(DatabaseError::InternalError("Update Primary key".into()));
                    }
                    //更新索引
                    if column.desc.is_unique && value != tuple.values[column.id().unwrap() as usize]
                    {
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
                                // transaction.add_index(
                                //     &table_name,
                                //     index,
                                //     vec![tuple.id.clone().unwrap()],
                                //     true,
                                // )?;
                                index_update_batch.push((
                                    index,
                                    vec![tuple.id.clone().unwrap()],
                                    true,
                                ));
                            }
                        }
                    }
                    // transaction.delete(&table_name, tuple.id.clone().unwrap())?;
                    tuple.values[column.id().unwrap() as usize] = value.clone();
                }
                update_batch.push((tuple, is_overwrite));
                // transaction.append(&table_name, tuple.clone(), is_overwrite)?;
            }
            for index_item in index_update_batch {
                transaction.add_index(&table_name, index_item.0, index_item.1, index_item.2)?;
            }
            for tuple in update_batch {
                transaction.append(&table_name, tuple.0, tuple.1)?;
            }
        }
        let tuple_builder = TupleBuilder::new_result();
        let tuple = tuple_builder.push_result("DELETE SUCCESS", &format!("{}", input_len))?;

        Ok(vec![tuple])
    }
}
