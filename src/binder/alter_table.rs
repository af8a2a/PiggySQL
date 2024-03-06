use crate::errors::*;
use sqlparser::ast::{AlterTableOperation, ObjectName};

use std::sync::Arc;

use super::{is_valid_identifier, Binder};
use crate::binder::{lower_case_name, split_name};
use crate::planner::operator::alter_table::AddColumnOperator;
use crate::planner::operator::alter_table::DropColumnOperator;
use crate::planner::operator::scan::ScanOperator;
use crate::planner::operator::Operator;
use crate::planner::LogicalPlan;
use crate::storage::Transaction;

impl<'a, T: Transaction> Binder<'a, T> {
    pub(crate) fn bind_alter_table(
        &mut self,
        name: &ObjectName,
        operation: &AlterTableOperation,
    ) -> Result<LogicalPlan> {
        let name = lower_case_name(name);
        let table_name: Arc<String> = Arc::new(split_name(&name)?.1.to_string());

        if let Some(table) = self.context.table(table_name.clone()) {
            let plan = match operation {
                AlterTableOperation::AddColumn {
                    column_keyword: _,
                    if_not_exists,
                    column_def,
                } => {
                    let plan = ScanOperator::build(table_name.clone(), &table);
                    let column = self.bind_column(column_def)?;
                    if !is_valid_identifier(column.name()) {
                        return Err(DatabaseError::InvalidColumn(
                            "illegal column naming".to_string(),
                        ));
                    }
                    LogicalPlan::new(
                        Operator::AddColumn(AddColumnOperator {
                            table_name,
                            if_not_exists: *if_not_exists,
                            column: self.bind_column(column_def)?,
                        }),
                        vec![plan],
                    )
                }
                AlterTableOperation::DropColumn {
                    column_name,
                    if_exists,
                    ..
                } => {
                    let plan = ScanOperator::build(table_name.clone(), &table);
                    let column_name = column_name.value.clone();
                    LogicalPlan::new(
                        Operator::DropColumn(DropColumnOperator {
                            table_name,
                            if_exists: *if_exists,
                            column_name,
                        }),
                        vec![plan],
                    )
                }
                AlterTableOperation::DropPrimaryKey => todo!(),
                AlterTableOperation::RenameColumn {
                    old_column_name: _,
                    new_column_name: _,
                } => todo!(),
                AlterTableOperation::RenameTable { table_name: _ } => todo!(),
                AlterTableOperation::ChangeColumn {
                    old_name: _,
                    new_name: _,
                    data_type: _,
                    options: _,
                } => todo!(),
                AlterTableOperation::AlterColumn {
                    column_name: _,
                    op: _,
                } => todo!(),
                _ => todo!(),
            };

            Ok(plan)
        } else {
            Err(DatabaseError::InvalidTable(format!(
                "not found table {}",
                table_name
            )))
        }
    }
}
