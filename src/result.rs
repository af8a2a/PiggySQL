use serde::Serialize;
use thiserror::Error as ThisError;



#[derive(ThisError, Serialize, Debug, PartialEq)]
pub enum Error {
    #[error("parser: {0}")]
    Parser(String),
}



pub type Result<T, E = Error> = std::result::Result<T, E>;
