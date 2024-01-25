use crate::{store::schema::ColumnRef, types::ValueRef};

#[derive(Debug, PartialEq, Clone)]
pub struct ValuesOperator {
    pub rows: Vec<Vec<ValueRef>>,
    pub columns: Vec<ColumnRef>,
}
