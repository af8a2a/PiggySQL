

use crate::catalog::{ColumnRef};
use crate::errors::*;
use crate::types::value::{DataValue, ValueRef};
use crate::types::LogicalType;

use std::sync::Arc;

use super::tuple::Tuple;

pub struct TupleBuilder {
    data_types: Vec<LogicalType>,
    data_values: Vec<ValueRef>,
    columns: Vec<ColumnRef>,
}

impl TupleBuilder {
    pub fn new(data_types: Vec<LogicalType>, columns: Vec<ColumnRef>) -> Self {
        TupleBuilder {
            data_types,
            data_values: Vec::new(),
            columns,
        }
    }

    pub fn new_result() -> Self {
        TupleBuilder {
            data_types: Vec::new(),
            data_values: Vec::new(),
            columns: Vec::new(),
        }
    }

    pub fn push_result(self, _header: &str, message: &str) -> Result<Tuple> {
        let values: Vec<ValueRef> = vec![Arc::new(DataValue::Utf8(Some(String::from(message))))];
        let t = Tuple {
            id: None,
            values,
        };
        Ok(t)
    }

    // pub fn push_str_row<'a>(
    //     &mut self,
    //     row: impl IntoIterator<Item = &'a str>,
    // ) -> Result<Option<Tuple>> {
    //     let mut primary_key_index = None;
    //     let mut tuple_map = HashMap::new();

    //     for (i, value) in row.into_iter().enumerate() {
    //         let data_value = DataValue::Utf8(Some(value.to_string()));
    //         let cast_data_value = data_value.cast(&self.data_types[i])?;
    //         self.data_values.push(Arc::new(cast_data_value.clone()));
    //         let col = &columns[i];
    //         col.id()
    //             .map(|col_id| tuple_map.insert(col_id, Arc::new(cast_data_value.clone())));
    //         if primary_key_index.is_none() && col.desc.is_primary {
    //             primary_key_index = Some(i);
    //         }
    //     }

    //     let primary_col_id = primary_key_index
    //         .map(|i| columns[i].id().unwrap())
    //         .ok_or_else(|| DatabaseError::PrimaryKeyNotFound)?;

    //     let tuple_id = tuple_map
    //         .get(&primary_col_id)
    //         .ok_or_else(|| DatabaseError::PrimaryKeyNotFound)?
    //         .clone();

    //     let tuple = if self.data_values.len() == self.data_types.len() {
    //         Some(Tuple {
    //             id: Some(tuple_id),
    //             values: self.data_values.clone(),
    //         })
    //     } else {
    //         None
    //     };
    //     Ok(tuple)
    // }

    pub fn build_with_row<'b>(
        &self,
        row: impl IntoIterator<Item = &'b str>,
    ) -> Result<Tuple> {
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
            return Err(DatabaseError::MisMatch("types".to_string(), "values".to_string()));
        }

        Ok(Tuple {
            id: primary_key,
            values,
        })
    }

}
