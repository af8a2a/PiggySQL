use std::collections::BTreeMap;

use super::{column::ColumnCatalog, table::{TableCatalog, TableName}, CatalogError};

#[derive(Debug, Clone)]
pub struct RootCatalog {
    table_idxs: BTreeMap<TableName, TableCatalog>,
}

impl Default for RootCatalog {
    fn default() -> Self {
        Self::new()
    }
}

impl RootCatalog {
    #[allow(dead_code)]
    pub fn new() -> Self {
        RootCatalog {
            table_idxs: Default::default(),
        }
    }

    pub(crate) fn get_table(&self, name: &String) -> Option<&TableCatalog> {
        self.table_idxs.get(name)
    }

    pub(crate) fn add_table(
        &mut self,
        table_name: TableName,
        columns: Vec<ColumnCatalog>,
    ) -> Result<TableName, CatalogError> {
        if self.table_idxs.contains_key(&table_name) {
            return Err(CatalogError::Duplicated("column", table_name.to_string()));
        }
        let table = TableCatalog::new(table_name.clone(), columns)?;

        self.table_idxs.insert(table_name.clone(), table);

        Ok(table_name)
    }

    pub(crate) fn drop_table(&mut self, table_name: &String) -> Result<(), CatalogError> {
        self.table_idxs
            .retain(|name, _| name.as_str() != table_name);

        Ok(())
    }
}
