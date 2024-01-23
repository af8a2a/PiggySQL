use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]

pub enum Value{
    Bool(bool),
    Interger(i64),
    Float(f32),
    String(String),
    Null
}