use crate::errors::DatabaseError;
use crate::planner::operator::copy_from_file::CopyFromFileOperator;
use crate::planner::operator::Operator;
use serde::{Deserialize, Serialize};
use sqlparser::ast::{CopyOption, CopySource, CopyTarget};
use std::{path::PathBuf, sync::Arc};

use super::*;

#[derive(Debug, PartialEq, PartialOrd, Ord, Hash, Eq, Clone, Serialize, Deserialize)]
pub struct ExtSource {
    pub path: PathBuf,
    pub format: FileFormat,
}

/// File format.
#[derive(Debug, PartialEq, PartialOrd, Ord, Hash, Eq, Clone, Serialize, Deserialize)]
pub enum FileFormat {
    Csv {
        /// Delimiter to parse.
        delimiter: char,
        /// Quote to use.
        quote: char,
        /// Escape character to use.
        escape: Option<char>,
        /// Whether or not the file has a header line.
        header: bool,
    },
}

impl std::fmt::Display for ExtSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}

impl std::fmt::Display for FileFormat {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}

impl<'a, T: Transaction> Binder<'a, T> {
    pub(super) fn bind_copy(
        &mut self,
        source: CopySource,
        target: CopyTarget,
        options: &[CopyOption],
    ) -> Result<LogicalPlan> {
        let (table_name, ..) = match source {
            CopySource::Table {
                table_name,
                columns,
            } => (table_name, columns),
            CopySource::Query(_) => {
                return Err(DatabaseError::UnsupportedCopySource(
                    "bad copy source".to_string(),
                ));
            }
        };

        if let Some(table) = self.context.table(Arc::new(table_name.to_string())) {
            let schema_ref = table.all_columns();
            let ext_source = ExtSource {
                path: match target {
                    CopyTarget::File { filename } => filename.into(),
                    t => todo!("unsupported copy target: {:?}", t),
                },
                format: FileFormat::from_options(options),
            };
            // COPY <dest_table> FROM <source_file>

            Ok(LogicalPlan::new(
                Operator::CopyFromFile(CopyFromFileOperator {
                    source: ext_source,
                    schema_ref,
                    table: table_name.to_string(),
                }),
                vec![],
            ))
        } else {
            Err(DatabaseError::InvalidTable(format!(
                "not found table {}",
                table_name
            )))
        }
    }
}

impl FileFormat {
    /// Create from copy options.
    pub fn from_options(options: &[CopyOption]) -> Self {
        let mut delimiter = ',';
        let mut quote = '"';
        let mut escape = None;
        let mut header = false;
        for opt in options {
            match opt {
                CopyOption::Format(fmt) => {
                    assert_eq!(fmt.value.to_lowercase(), "csv", "only support CSV format")
                }
                CopyOption::Delimiter(c) => delimiter = *c,
                CopyOption::Header(b) => header = *b,
                CopyOption::Quote(c) => quote = *c,
                CopyOption::Escape(c) => escape = Some(*c),
                o => panic!("unsupported copy option: {:?}", o),
            }
        }
        FileFormat::Csv {
            delimiter,
            quote,
            escape,
            header,
        }
    }
}
