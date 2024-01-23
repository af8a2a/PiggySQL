pub mod ast_literal;
pub mod expr;
pub mod join;
pub mod query;
pub mod types;
pub mod ddl;
pub mod aggregate_function;
pub mod operator;
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
