use bigdecimal::BigDecimal;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum AstLiteral {
    Boolean(bool),
    Number(BigDecimal),
    QuotedString(String),
    HexString(String),
    Null,
}
