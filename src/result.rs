use serde::Serialize;
use thiserror::Error as ThisError;

use crate::types::translate::error::TranslateError;

#[derive(ThisError, Serialize, Debug, PartialEq)]
pub enum Error {
    #[error("storage: {0}")]
    StorageMsg(String),

    #[error("parser: {0}")]
    Parser(String),

    #[error("translate: {0}")]
    Translate(#[from] TranslateError),
}

pub type Result<T, E = Error> = std::result::Result<T, E>;
