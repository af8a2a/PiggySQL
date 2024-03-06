// Module: catalog

pub(crate) use self::column::*;
pub(crate) use self::table::*;

pub(crate) static DEFAULT_DATABASE_NAME: &str = "Piggysql";
pub(crate) static DEFAULT_SCHEMA_NAME: &str = "Piggysql";

mod column;
mod table;
