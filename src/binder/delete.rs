use crate::binder::{lower_case_name, split_name, BindError, Binder};
use crate::plan::operator::delete::DeleteOperator;
use crate::plan::operator::Operator;
use crate::plan::LogicalPlan;
use crate::storage::Transaction;
use sqlparser::ast::{Expr, TableFactor, TableWithJoins};

impl<'a, T: Transaction> Binder<'a, T> {
    pub(crate) fn bind_delete(
        &mut self,
        from: &TableWithJoins,
        selection: &Option<Expr>,
    ) -> Result<LogicalPlan, BindError> {
        if let TableFactor::Table { name, alias, .. } = &from.relation {
            let name = lower_case_name(name);
            let (_, name) = split_name(&name)?;
            let (table_name, mut plan) =
                self._bind_single_table_ref(None, name, Self::trans_alias(alias))?;

            if let Some(predicate) = selection {
                plan = self.bind_where(plan, predicate)?;
            }

            Ok(LogicalPlan {
                operator: Operator::Delete(DeleteOperator { table_name }),
                childrens: vec![plan],
            })
        } else {
            unreachable!("only table")
        }
    }
}
