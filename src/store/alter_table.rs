use {
    super::schema::Column, crate::result::{Error, Result}
};

// /// AlterTable
pub trait AlterTable {
    async fn rename_schema(&mut self, _table_name: &str, _new_table_name: &str) -> Result<()> {
        let msg = "[Storage] AlterTable::rename_schema is not supported".to_owned();

        Err(Error::StorageMsg(msg))
    }
    async fn rename_column(
        &mut self,
        _table_name: &str,
        _old_column_name: &str,
        _new_column_name: &str,
    ) -> Result<()> {
        let msg = "[Storage] AlterTable::rename_column is not supported".to_owned();

        Err(Error::StorageMsg(msg))
    }

    async fn add_column(&mut self, _table_name: &str, _column_def: &Column) -> Result<()> {
        let msg = "[Storage] AlterTable::add_column is not supported".to_owned();

        Err(Error::StorageMsg(msg))
    }

    async fn drop_column(
        &mut self,
        _table_name: &str,
        _column_name: &str,
        _if_exists: bool,
    ) -> Result<()> {
        let msg = "[Storage] AlterTable::drop_column is not supported".to_owned();

        Err(Error::StorageMsg(msg))
    }
}
