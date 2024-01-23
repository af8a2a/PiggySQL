use crate::{
    ast::query::OrderByExpr, result::{Error, Result}, types::{operator::IndexOperator, Value}
};

use super::RowIter;

pub trait Index {
    async fn scan_indexed_data(
        &self,
        _table_name: &str,
        _index_name: &str,
        _asc: Option<bool>,
        _cmp_value: Option<(&IndexOperator, Value)>,
    ) -> Result<RowIter> {
        Err(Error::StorageMsg(
            "[Storage] Index::scan_indexed_data is not supported".to_owned(),
        ))
    }
}

pub trait IndexMut {
    async fn create_index(
        &mut self,
        _table_name: &str,
        _index_name: &str,
        _column: &OrderByExpr,
    ) -> Result<()> {
        let msg = "[Storage] Index::create_index is not supported".to_owned();

        Err(Error::StorageMsg(msg))
    }

    async fn drop_index(&mut self, _table_name: &str, _index_name: &str) -> Result<()> {
        let msg = "[Storage] Index::drop_index is not supported".to_owned();

        Err(Error::StorageMsg(msg))
    }
}
