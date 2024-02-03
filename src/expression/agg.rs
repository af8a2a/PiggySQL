
use std::fmt::Display;

use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Eq, Clone, Hash, Serialize, Deserialize)]

pub enum Aggregate {
    Avg,
    Max,
    Min,
    Sum,
    Count,
}
impl Aggregate {
    pub fn allow_distinct(&self) -> bool {
        match self {
            Aggregate::Avg => false,
            Aggregate::Max => false,
            Aggregate::Min => false,
            Aggregate::Sum => true,
            Aggregate::Count => true,
        }
    }
}

