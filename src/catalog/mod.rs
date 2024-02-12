// Module: catalog
use std::sync::Arc;

pub(crate) use self::column::*;
pub(crate) use self::root::*;
pub(crate) use self::table::*;

/// The type of catalog reference.
pub type RootRef = Arc<RootCatalog>;

pub(crate) static DEFAULT_DATABASE_NAME: &str = "Piggysql";
pub(crate) static DEFAULT_SCHEMA_NAME: &str = "Piggysql";

mod column;
mod root;
mod table;
