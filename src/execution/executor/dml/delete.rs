use crate::catalog::TableName;
use crate::execution::executor::{build, Executor, Source};

use crate::planner::operator::delete::DeleteOperator;
use crate::planner::LogicalPlan;
use crate::storage::Transaction;
use crate::types::index::Index;
use crate::types::tuple_builder::TupleBuilder;

use itertools::Itertools;

pub struct Delete {
    table_name: TableName,
    input: LogicalPlan,
}

impl From<(DeleteOperator, LogicalPlan)> for Delete {
    fn from((DeleteOperator { table_name }, input): (DeleteOperator, LogicalPlan)) -> Self {
        Delete { table_name, input }
    }
}

impl<T: Transaction> Executor<T> for Delete {
    fn execute(self, transaction: &mut T) -> Source {
        let Delete { table_name, input } = self;
        let input = build(input, transaction)?;
        let option_index_metas = transaction.table(table_name.clone()).map(|table_catalog| {
            table_catalog
                .all_columns()
                .into_iter()
                .enumerate()
                .filter_map(|(i, col)| {
                    col.desc
                        .is_unique
                        .then(|| {
                            col.id().and_then(|col_id| {
                                table_catalog
                                    .get_unique_index(&col_id)
                                    .map(|index_meta| (i, index_meta.clone()))
                            })
                        })
                        .flatten()
                })
                .collect_vec()
        });
        if let Some(index_metas) = option_index_metas {
            for tuple in input.iter() {
                for (i, index_meta) in index_metas.iter() {
                    let value = &tuple.values[*i];

                    if !value.is_null() {
                        let index = Index {
                            id: index_meta.id,
                            column_values: vec![value.clone()],
                        };

                        transaction.del_index(&table_name, &index)?;
                    }
                }

                if let Some(tuple_id) = tuple.id.clone() {
                    transaction.delete(&table_name, tuple_id)?;
                }
            }
        }
        let tuple_builder = TupleBuilder::new_result();
        let tuple = tuple_builder.push_result("DELETE SUCCESS", &format!("{}", input.len()))?;
        Ok(vec![tuple])
    }
}
