use serde::{Deserialize, Serialize};

use crate::types::expression::Expr;

use super::query::TableFactor;

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Join {
    pub relation: TableFactor,
    pub join_operator: JoinOperator,
    pub join_executor: JoinExecutor,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum JoinOperator {
    Inner(JoinConstraint),
    LeftOuter(JoinConstraint),
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum JoinConstraint {
    On(Expr),
    None,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TableWithJoins {
    pub relation: TableFactor,
    pub joins: Vec<Join>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum JoinExecutor {
    NestedLoop,
    Hash {
        key_expr: Expr,
        value_expr: Expr,
        where_clause: Option<Expr>,
    },
}
