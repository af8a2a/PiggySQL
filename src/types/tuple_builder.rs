use crate::catalog::ColumnRef;
use crate::errors::*;
use crate::types::value::{DataValue, ValueRef};

use std::sync::Arc;

use super::tuple::Tuple;

pub struct TupleBuilder {
    columns: Vec<ColumnRef>,
}

impl TupleBuilder {
    pub fn new( columns: Vec<ColumnRef>) -> Self {
        TupleBuilder {
            columns,
        }
    }

    pub fn new_result() -> Self {
        TupleBuilder {
            columns: Vec::new(),
        }
    }

    pub fn push_result(self, _header: &str, message: &str) -> Result<Tuple> {
        let values: Vec<ValueRef> = vec![Arc::new(DataValue::Utf8(Some(String::from(message))))];
        let t = Tuple { id: None, values };
        Ok(t)
    }

    pub fn build_with_row<'b>(&self, row: impl IntoIterator<Item = &'b str>) -> Result<Tuple> {
        let mut values = Vec::with_capacity(self.columns.len());
        let mut primary_key = None;

        for (i, value) in row.into_iter().enumerate() {
            // debug!("{}: {}", i, value);
            let data_value = Arc::new(
                DataValue::Utf8(Some(value.to_string())).cast(self.columns[i].datatype())?,
            );

            if primary_key.is_none() && self.columns[i].desc.is_primary {
                primary_key = Some(data_value.clone());
            }
            values.push(data_value);
        }
        if values.len() != self.columns.len() {
            return Err(DatabaseError::MisMatch(
                "types".to_string(),
                "values".to_string(),
            ));
        }

        Ok(Tuple {
            id: primary_key,
            values,
        })
    }
}
