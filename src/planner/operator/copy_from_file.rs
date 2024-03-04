use crate::binder::copy::ExtSource;
use crate::catalog::ColumnRef;
use itertools::Itertools;
use std::fmt;
use std::fmt::Formatter;

#[derive(Debug, PartialEq, Eq, Clone, Hash)]
pub struct CopyFromFileOperator {
    pub table: String,
    pub source: ExtSource,
    pub schema_ref: Vec<ColumnRef>,
}

impl fmt::Display for CopyFromFileOperator {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        let columns = self
            .schema_ref
            .iter()
            .map(|column| column.name().to_string())
            .join(", ");
        write!(
            f,
            "Copy {} -> {} [{}]",
            self.source.path.display(),
            self.table,
            columns
        )?;

        Ok(())
    }
}
