use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Eq, Clone, Serialize, Deserialize)]

pub enum Aggregate {
    Avg,
    Max,
    Min,
    Sum,
    Count,
}
