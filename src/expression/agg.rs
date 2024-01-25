use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Eq, Clone, Hash, Serialize, Deserialize)]

pub enum Aggregate {
    Avg,
    Max,
    Min,
    Sum,
    Count,
}
