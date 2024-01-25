use std::sync::Arc;

use serde::{Deserialize, Serialize};



pub type ValueRef = Arc<DataValue>;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]

pub enum DataValue {
    Null,
    Boolean(Option<bool>),
    Float(Option<f32>),
    Interger(Option<i8>),
    Utf8(Option<String>),
}
#[derive(Debug, PartialEq, Eq, Clone, Serialize, Deserialize)]

pub enum DataType {
    Null,
    Boolean,
    Float,
    Interger,
    Utf8,

}