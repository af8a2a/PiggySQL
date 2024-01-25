use serde::Serialize;
use thiserror::Error as ThisError;


#[derive(ThisError, Serialize, Debug, PartialEq)]
pub enum Error {
    #[error("storage: {0}")]
    StorageMsg(String),

    #[error("parser: {0}")]
    Parser(String),
    #[error("Bind: {0}")]
    BindError(String),
    // #[error("evaluate: {0}")]
    // Evaluate(#[from] EvaluateError),
}

pub type Result<T, E = Error> = std::result::Result<T, E>;
