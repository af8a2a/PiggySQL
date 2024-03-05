use crate::errors::*;
use std::sync::Arc;

use itertools::Itertools;
use sqlparser::ast::{ObjectName, OrderByExpr};

use crate::{
    binder::lower_case_name,
    planner::{
        operator::{create_index::CreateIndexOperator, Operator},
        LogicalPlan,
    },
    storage::Transaction,
};

use super::Binder;

impl<'a, T: Transaction> Binder<'a, T> {
    pub(crate) fn bind_create_index(
        &mut self,
        index_name: &ObjectName,
        table_name: &ObjectName,
        columns: &Vec<OrderByExpr>,
    ) -> Result<LogicalPlan> {
        // let table_name = lower_case_name(table_name);
        // let index_name = lower_case_name(index_name);
        let table_name = Arc::new(table_name.to_string());
        let index_name = Arc::new(index_name.to_string());

        let col_ident = columns
            .iter()
            .filter_map(|col| match &col.expr {
                sqlparser::ast::Expr::Identifier(ident) => Some(ident.to_string()),
                _ => unreachable!(),
            })
            .collect_vec();
        //now only support unique index
        assert_eq!(col_ident.len(), 1);
        let col_name = col_ident[0].clone();
        Ok(LogicalPlan::new(
            Operator::CreateIndex(CreateIndexOperator {
                table_name,
                index_name,
                col_name,
            }),
            vec![],
        ))
    }
}
