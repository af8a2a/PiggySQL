use crate::binder::{lower_case_name, split_name, BindError, Binder};
use crate::expression::ScalarExpression;
use crate::planner::operator::update::UpdateOperator;
use crate::planner::operator::Operator;
use crate::planner::LogicalPlan;
use crate::storage::Transaction;
use crate::types::value::ValueRef;
use sqlparser::ast::{Assignment, Expr, TableFactor, TableWithJoins};
use std::slice;
use std::sync::Arc;

impl<'a, T: Transaction> Binder<'a, T> {
    pub(crate) fn bind_update(
        &mut self,
        to: &TableWithJoins,
        selection: &Option<Expr>,
        assignments: &[Assignment],
    ) -> Result<LogicalPlan, BindError> {
        if let TableFactor::Table { name, .. } = &to.relation {
            let name = lower_case_name(name);
            let (_, name) = split_name(&name)?;
            let table_name = Arc::new(name.to_string());

            let mut plan = self.bind_table_ref(slice::from_ref(to))?;

            if let Some(predicate) = selection {
                plan = self.bind_where(plan, predicate)?;
            }

            let bind_table_name = Some(table_name.to_string());

            let mut columns = Vec::with_capacity(assignments.len());

            let mut set_expr = Vec::with_capacity(assignments.len());

            for assignment in assignments {
                //set x= expr,  the expr as value
                let value = self.bind_expr(&assignment.value)?;

                for ident in &assignment.id {
                    match self.bind_column_ref_from_identifiers(
                        slice::from_ref(ident),
                        bind_table_name.as_ref(),
                    )? {
                        ScalarExpression::ColumnRef(catalog) => {
                            columns.push(catalog);
                            set_expr.push(value.clone());
                        }
                        _ => unreachable!(),
                    }
                }
            }
            // let values_plan = self.bind_values(vec![], columns);

            Ok(LogicalPlan {
                operator: Operator::Update(UpdateOperator { columns,set_expr, table_name }),
                childrens: vec![plan],
            })
        } else {
            unreachable!("only table")
        }
    }
}
