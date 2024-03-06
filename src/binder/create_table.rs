use crate::errors::*;
use itertools::Itertools;
use sqlparser::ast::{ColumnDef, ColumnOption, ObjectName, TableConstraint};
use std::collections::HashSet;
use std::sync::Arc;

use super::{is_valid_identifier, Binder};
use crate::binder::{lower_case_name, split_name};
use crate::catalog::{ColumnCatalog, ColumnDesc};
use crate::expression::ScalarExpression;
use crate::planner::operator::create_table::CreateTableOperator;
use crate::planner::operator::Operator;
use crate::planner::LogicalPlan;
use crate::storage::Transaction;
use crate::types::value::DataValue;
use crate::types::LogicalType;

impl<'a, T: Transaction> Binder<'a, T> {
    // TODO: TableConstraint
    pub(crate) fn bind_create_table(
        &mut self,
        name: &ObjectName,
        columns: &[ColumnDef],
        constraints: &[TableConstraint],
        if_not_exists: bool,
    ) -> Result<LogicalPlan> {
        let name = lower_case_name(name);
        let (_, name) = split_name(&name)?;
        let table_name = Arc::new(name.to_string());

        if !is_valid_identifier(&table_name) {
            return Err(DatabaseError::InvalidTable(
                "illegal table naming".to_string(),
            ));
        }

        {
            // check duplicated column names
            let mut set = HashSet::new();
            for col in columns.iter() {
                let col_name = &col.name.value;
                if !set.insert(col_name.clone()) {
                    return Err(DatabaseError::AmbiguousColumn(col_name.to_lowercase()));
                }
                if !is_valid_identifier(col_name) {
                    return Err(DatabaseError::InvalidColumn(
                        "illegal column naming".to_string(),
                    ));
                }
            }
        }
        let mut columns: Vec<ColumnCatalog> = columns
            .iter()
            .map(|col| self.bind_column(col))
            .try_collect()?;
        for constraint in constraints {
            match constraint {
                TableConstraint::Unique {
                    columns: column_names,
                    is_primary,
                    ..
                } => {
                    for column_name in column_names {
                        if let Some(column) = columns
                            .iter_mut()
                            .find(|column| column.name() == column_name.to_string())
                        {
                            if *is_primary {
                                column.desc.is_primary = true;
                            } else {
                                column.desc.is_unique = true;
                            }
                        }
                    }
                }
                _ => todo!(),
            }
        }
        //experiment
        //allow table no primary key
        // if columns.iter().filter(|col| col.desc.is_primary).count() != 1 {
        //     return Err(DatabaseError::InvalidTable(
        //         "The primary key field must exist and have at least one".to_string(),
        //     ));
        // }

        Ok(LogicalPlan::new(
            Operator::CreateTable(CreateTableOperator {
                table_name,
                columns,
                if_not_exists,
            }),
            vec![],
        ))
    }

    pub fn bind_column(&mut self, column_def: &ColumnDef) -> Result<ColumnCatalog> {
        let column_name = column_def.name.to_string();
        let mut column_desc = ColumnDesc::new(
            LogicalType::try_from(column_def.data_type.clone())?,
            false,
            false,
            None,
        );
        let mut nullable = true;

        // TODO: 这里可以对更多字段可设置内容进行补充
        for option_def in &column_def.options {
            match &option_def.option {
                ColumnOption::Null => nullable = true,
                ColumnOption::NotNull => nullable = false,
                ColumnOption::Unique { is_primary } => {
                    if *is_primary {
                        column_desc.is_primary = true;
                        nullable = false;
                        // Skip other options when using primary key
                        break;
                    } else {
                        column_desc.is_unique = true;
                    }
                }
                ColumnOption::Default(expr) => {
                    if let ScalarExpression::Constant(value) = self.bind_expr(expr)? {
                        let cast_value =
                            DataValue::clone(&value).cast(&column_desc.column_datatype)?;
                        column_desc.default = Some(Arc::new(cast_value));
                    } else {
                        unreachable!("'default' only for constant")
                    }
                }
                _ => todo!(),
            }
        }

        Ok(ColumnCatalog::new(column_name, nullable, column_desc, None))
    }
}
