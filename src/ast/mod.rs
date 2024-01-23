pub mod query;
pub mod join;

use serde::{Deserialize, Serialize};




#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Statement {
        /// SELECT, VALUES
        Query(

        ),

}