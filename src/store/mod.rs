use crate::result::{Error, Result};
use crate::types::DataType;
use futures::Stream;
use sqlparser::ast::{ColumnDef, Value};
use std::pin::Pin;

use self::data_row::DataRow;

use self::{
    alter_table::AlterTable,
    index::{Index, IndexMut},
    metadata::Metadata,
    transaction::Transaction,
};

mod alter_table;
mod data_row;
mod index;
mod metadata;
mod transaction;

type Key = Value;
pub type RowIter = Pin<Box<dyn Stream<Item = Result<(Key, DataRow)>>>>;

pub struct Schema{
    pub table_name: String,
    pub columns: Vec<Column>,
}
pub struct Column{
    pub column_name: String,
    pub data_type: DataType,
}

// pub trait Storage: Store + Index + Metadata {}
// impl<S: Store + Index + Metadata> Storage for S {}

// pub trait StorageMut: StoreMut + IndexMut + AlterTable + Transaction {}
// impl<S: StoreMut + IndexMut + AlterTable + Transaction> StorageMut for S {}

// pub trait Store {
//     async fn fetch_schema(&self, table_name: &str) -> Result<Option<Schema>>;

//     async fn fetch_all_schemas(&self) -> Result<Vec<Schema>>;

//     async fn fetch_data(&self, table_name: &str, key: &Key) -> Result<Option<DataRow>>;

//     async fn scan_data(&self, table_name: &str) -> Result<RowIter>;
// }

// pub trait StoreMut {
//     async fn insert_schema(&mut self, _schema: &Schema) -> Result<()> {
//         let msg = "[Storage] StoreMut::insert_schema is not supported".to_owned();

//         Err(Error::StorageMsg(msg))
//     }

//     async fn delete_schema(&mut self, _table_name: &str) -> Result<()> {
//         let msg = "[Storage] StoreMut::delete_schema is not supported".to_owned();

//         Err(Error::StorageMsg(msg))
//     }

//     async fn append_data(&mut self, _table_name: &str, _rows: Vec<DataRow>) -> Result<()> {
//         let msg = "[Storage] StoreMut::append_data is not supported".to_owned();

//         Err(Error::StorageMsg(msg))
//     }

//     async fn insert_data(&mut self, _table_name: &str, _rows: Vec<(Key, DataRow)>) -> Result<()> {
//         let msg = "[Storage] StoreMut::insert_data is not supported".to_owned();

//         Err(Error::StorageMsg(msg))
//     }

//     async fn delete_data(&mut self, _table_name: &str, _keys: Vec<Key>) -> Result<()> {
//         let msg = "[Storage] StoreMut::delete_data is not supported".to_owned();

//         Err(Error::StorageMsg(msg))
//     }
// }
