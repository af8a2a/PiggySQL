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
    #[error("Internal: {0}")]
    Internal(String),
    #[error("Value: {0}")]
    Value(String),
}
impl From<std::array::TryFromSliceError> for Error {
    fn from(err: std::array::TryFromSliceError) -> Self {
        Error::Internal(err.to_string())
    }
}

impl From<std::string::FromUtf8Error> for Error {
    fn from(err: std::string::FromUtf8Error) -> Self {
        Error::Internal(err.to_string())
    }
}

pub type Result<T, E = Error> = std::result::Result<T, E>;
