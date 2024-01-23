pub mod query;
pub mod join;

use serde::{Deserialize, Serialize};


use self::query::Query;




#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Statement {
    /// SELECT, VALUES
    Query(Query),
    /// INSERT
    Insert {
        /// TABLE
        table_name: String,
        /// COLUMNS
        columns: Vec<String>,
        /// A SQL query that specifies what to insert
        source: Query,
    },

}