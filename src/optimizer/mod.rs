use crate::types::errors::TypeError;

mod core;
pub mod heuristic;
pub mod rule;

#[derive(thiserror::Error, Debug)]
pub enum OptimizerError {
    #[error("type error")]
    TypeError(
        #[source]
        #[from]
        TypeError,
    ),
}
