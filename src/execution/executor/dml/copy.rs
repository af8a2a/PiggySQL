use crate::binder::copy::FileFormat;
use crate::errors::*;
use crate::execution::executor::{Executor, Source};
use crate::planner::operator::copy_from_file::CopyFromFileOperator;
use crate::storage::Transaction;
use crate::types::tuple::Tuple;
use crate::types::tuple_builder::TupleBuilder;
use csv::Terminator;
use itertools::Itertools;
use std::fs::File;
use std::io::BufReader;
use tracing::debug;
#[allow(dead_code)]
pub struct CopyFromFile {
    op: CopyFromFileOperator,
    size: usize,
}
impl From<CopyFromFileOperator> for CopyFromFile {
    fn from(op: CopyFromFileOperator) -> Self {
        CopyFromFile { op, size: 0 }
    }
}

impl<T: Transaction> Executor<T> for CopyFromFile {
    fn execute(self, transaction: &mut T) -> Source {
        let table_name = self.op.table.clone();
        let tuples = self.read_file_blocking()?;
        let mut size = 0_usize;
        for tuple in tuples {
            transaction.append(&table_name, tuple, false)?;
            size += 1;
        }

        let res = return_result(size)?;
        Ok(vec![res])
    }
}
fn return_result(size: usize) -> Result<Tuple> {
    let builder = TupleBuilder::new_result();
    let tuple = builder.push_result("COPY FROM SOURCE", &format!("import {} rows", size))?;

    Ok(tuple)
}

impl CopyFromFile {
    /// Read records from file using blocking IO.
    fn read_file_blocking(mut self) -> Result<Vec<Tuple>> {
        let file = File::open(self.op.source.path)?;
        let mut buf_reader = BufReader::new(file);
        let mut reader = match self.op.source.format {
            FileFormat::Csv {
                delimiter: _,
                quote,
                escape,
                header,
            } => csv::ReaderBuilder::new()
                .delimiter(b'|')
                .quote(quote as u8)
                .escape(escape.map(|c| c as u8))
                .has_headers(header)
                .terminator(Terminator::CRLF)
                .from_reader(&mut buf_reader),
        };

        let column_count = self.op.schema_ref.len();

        debug!("column count: {}", column_count);
        let tuple_builder = TupleBuilder::new(self.op.schema_ref.clone());
        let mut tuples = vec![];
        for record in reader.records() {
            // read records and push raw str rows into data chunk builder
            let record = record?;

            if !(record.len() == column_count
                || record.len() == column_count + 1 && record.get(column_count) == Some(""))
            {
                return Err(DatabaseError::LengthMismatch {
                    expected: column_count,
                    actual: record.len(),
                });
            }

            self.size += 1;
            tuples.push(tuple_builder.build_with_row(record.iter().take(column_count))?);
        }
        Ok(tuples)
    }
}
