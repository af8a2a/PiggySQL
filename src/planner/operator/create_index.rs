use crate::{catalog::TableName, types::index::{IndexMeta, IndexMetaRef}};

#[derive(Debug, PartialEq, Clone)]
pub struct CreateIndexOperator {
    pub table_name: TableName,
    pub index_info: IndexMetaRef,
}
