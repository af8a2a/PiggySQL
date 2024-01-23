use {
    crate::{result::Result, types::Value},
    std::{collections::HashMap, iter::empty},
};

type ObjectName = String;
pub type MetaIter = Box<dyn Iterator<Item = Result<(ObjectName, HashMap<String, Value>)>>>;

pub trait Metadata {
    async fn scan_table_meta(&self) -> Result<MetaIter> {
        Ok(Box::new(empty()))
    }
}
