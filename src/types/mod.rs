use std::{hash::Hash, sync::Arc};

use ordered_float::OrderedFloat;
use serde::{Deserialize, Serialize};

pub type ValueRef = Arc<DataValue>;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]

pub enum DataValue {
    Null,
    Boolean(Option<bool>),
    Float(Option<f32>),
    Interger(Option<i8>),
    Utf8(Option<String>),
}
impl Eq for DataValue {}
impl Hash for DataValue {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        use DataValue::*;
        match self {
            Boolean(v) => v.hash(state),
            Utf8(v) => v.hash(state),
            Float(v) => {
                let v = v.map(OrderedFloat);
                v.hash(state)
            }

            Null => 1.hash(state),
            Interger(v) => v.hash(state),
        }
    }
}

#[derive(Debug, PartialEq, Hash, Eq, Clone, Serialize, Deserialize)]

pub enum DataType {
    Null,
    Boolean,
    Float,
    Interger,
    Utf8,
}

impl DataType {
    fn try_from(value: sqlparser::ast::DataType) -> Result<Self, Self::Error> {
        match value {
            sqlparser::ast::DataType::Float(_) => Ok(DataType::Float),
            sqlparser::ast::DataType::Int(_) | sqlparser::ast::DataType::Integer(_) => {
                Ok(DataType::Interger)
            }
            sqlparser::ast::DataType::Boolean => Ok(DataType::Boolean),
            _=>unimplemented!()
        }
    }
}
