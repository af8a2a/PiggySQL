use itertools::Itertools;

use crate::catalog::ColumnRef;

use self::{
    aggregate::AggregateOperator, alter_table::{AddColumnOperator, DropColumnOperator}, create_table::CreateTableOperator, delete::DeleteOperator, drop_table::DropTableOperator, filter::FilterOperator, insert::InsertOperator, join::{JoinCondition, JoinOperator}, limit::LimitOperator, project::ProjectOperator, scan::ScanOperator, sort::SortOperator, update::UpdateOperator, values::ValuesOperator
};

pub mod aggregate;
pub mod alter_table;
pub mod create_table;
pub mod delete;
pub mod drop_table;
pub mod filter;
pub mod insert;
pub mod join;
pub mod limit;
pub mod project;
pub mod scan;
pub mod sort;
pub mod update;
pub mod values;
#[derive(Debug, PartialEq, Clone)]
pub enum Operator {
    // DQL
    Dummy,
    Aggregate(AggregateOperator),
    Filter(FilterOperator),
    Join(JoinOperator),
    Project(ProjectOperator),
    Scan(ScanOperator),
    Sort(SortOperator),
    Limit(LimitOperator),
    Values(ValuesOperator),
    // DML
    Insert(InsertOperator),
    Update(UpdateOperator),
    Delete(DeleteOperator),
    // DDL
    AddColumn(AddColumnOperator),
    DropColumn(DropColumnOperator),
    CreateTable(CreateTableOperator),
    DropTable(DropTableOperator),
}
impl Operator {
    pub fn referenced_columns(&self, only_column_ref: bool) -> Vec<ColumnRef> {
        match self {
            Operator::Aggregate(op) => op
                .agg_calls
                .iter()
                .chain(op.groupby_exprs.iter())
                .flat_map(|expr| expr.referenced_columns(only_column_ref))
                .collect_vec(),
            Operator::Filter(op) => op.predicate.referenced_columns(only_column_ref),
            Operator::Join(op) => {
                let mut exprs = Vec::new();

                if let JoinCondition::On { on, filter } = &op.on {
                    for (left_expr, right_expr) in on {
                        exprs.append(&mut left_expr.referenced_columns(only_column_ref));
                        exprs.append(&mut right_expr.referenced_columns(only_column_ref));
                    }

                    if let Some(filter_expr) = filter {
                        exprs.append(&mut filter_expr.referenced_columns(only_column_ref));
                    }
                }
                exprs
            }
            Operator::Project(op) => op
                .exprs
                .iter()
                .flat_map(|expr| expr.referenced_columns(only_column_ref))
                .collect_vec(),
            Operator::Scan(op) => op
                .columns
                .iter()
                .flat_map(|expr| expr.referenced_columns(only_column_ref))
                .collect_vec(),
            Operator::Sort(op) => op
                .sort_fields
                .iter()
                .map(|field| &field.expr)
                .flat_map(|expr| expr.referenced_columns(only_column_ref))
                .collect_vec(),
            Operator::Values(op) => op.columns.clone(),
            _ => vec![],
        }
    }
}
