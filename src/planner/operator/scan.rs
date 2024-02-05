use super::Operator;
use crate::catalog::{TableCatalog, TableName};
use crate::expression::simplify::ConstantBinary;
use crate::expression::ScalarExpression;
use crate::planner::LogicalPlan;
use crate::storage::Bounds;
use crate::types::index::IndexMetaRef;
use itertools::Itertools;
use std::fmt;
use std::fmt::Formatter;

#[derive(Debug, PartialEq, Clone)]
pub struct ScanOperator {
    pub index_metas: Vec<IndexMetaRef>,

    pub table_name: TableName,
    pub columns: Vec<ScalarExpression>,
    // Support push down limit.
    pub limit: Bounds,

    // IndexScan only
    // Support push down predicate.
    // If pre_where is simple predicate, for example:  a > 1 then can calculate directly when read data.
    pub index_by: Option<(IndexMetaRef, Vec<ConstantBinary>)>,
}
impl ScanOperator {
    pub fn build(table_name: TableName, table_catalog: &TableCatalog) -> LogicalPlan {
        // Fill all Columns in TableCatalog by default
        let columns = table_catalog
            .all_columns()
            .into_iter()
            .map(ScalarExpression::ColumnRef)
            .collect_vec();

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
impl fmt::Display for ScanOperator {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        let projection_columns = self
            .columns
            .iter()
            .map(|column| format!("{}", column))
            .join(", ");
        let (offset, limit) = self.limit;
        if let Some(index) = &self.index_by {
            write!(
                f,
                "IndexScan {} by {} -> [{}]",
                self.table_name, index.0.name, projection_columns
            )?;
        } else {
            write!(f, "Scan {} -> [{}]", self.table_name, projection_columns)?;
        }
        if let Some(limit) = limit {
            write!(f, ", Limit: {}", limit)?;
        }
        if let Some(offset) = offset {
            write!(f, ", Offset: {}", offset)?;
        }

        Ok(())
    }
}
