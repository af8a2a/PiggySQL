use crate::{
    result::{Error, Result},
    types::{expression::IndexOperator, Value},
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
