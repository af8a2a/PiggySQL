use std::sync::Arc;

use crate::{
    expression::ScalarExpression,
    plan::{LogicalPlan, TableName},
    store::schema::{IndexOperator, Schema, SchemaIndex},
};

use super::Operator;
type Bounds = (Option<usize>, Option<usize>);

#[derive(Debug, PartialEq, Clone)]
pub struct ScanOperator {
    pub index_metas: Vec<SchemaIndex>,

    pub table_name: TableName,
    pub columns: Vec<ScalarExpression>,
    // Support push down limit.
    pub limit: Bounds,

    // IndexScan only
    // Support push down predicate.
    // If pre_where is simple predicate, for example:  a > 1 then can calculate directly when read data.
    pub index_by: Option<(IndexOperator, ScalarExpression)>,
}
impl ScanOperator {
    pub fn build(table_name: TableName, table_catalog: &Schema) -> LogicalPlan {
        // Fill all Columns in TableCatalog by default
        let columns = table_catalog
            .columns
            .iter()
            .cloned()
            .map(Arc::new)
            .map(ScalarExpression::ColumnRef)
            .collect();

        LogicalPlan {
            operator: Operator::Scan(ScanOperator {
                index_metas: table_catalog.indexes.clone(),
                table_name,
                columns,
                limit: (None, None),
                index_by: None,
            }),
            childrens: vec![],
        }
    }
}
