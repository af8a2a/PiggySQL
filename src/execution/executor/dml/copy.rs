use crate::binder::copy::FileFormat;
use crate::execution::executor::{Executor, Source};
use crate::planner::operator::copy_from_file::CopyFromFileOperator;
use crate::storage::Transaction;
use crate::types::tuple::Tuple;
use crate::types::tuple_builder::TupleBuilder;
use futures::executor::{block_on, block_on_stream};
use itertools::Itertools;
use std::fs::File;
use std::io::BufReader;
use tokio::sync::mpsc::Sender;
use tokio::task::block_in_place;
use crate::errors::*;
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
        let (tx, mut rx) = tokio::sync::mpsc::channel(1);
        let (tx1, mut rx1) = tokio::sync::mpsc::channel(1);
        let table_name = self.op.table.clone();
        let handle = tokio::task::spawn_blocking(|| self.read_file_blocking(tx));
        let mut size = 0_usize;

        while let Some(chunk) = block_on(rx.recv()) {
            transaction.append(&table_name, chunk, false)?;
            size += 1;
        }
        block_on(handle)?;

        let handle = tokio::task::spawn_blocking(move || return_result(size, tx1));
        let mut tuples=vec![];
        while let Some(chunk) =block_on(rx1.recv()) {
            tuples.push(chunk);
        }
        block_on(handle)?;

        Ok(tuples)
    }
}
fn return_result(size: usize, tx: Sender<Tuple>) -> Result<()> {
    let builder = TupleBuilder::new_result();
    let tuple = builder.push_result("COPY FROM SOURCE", &format!("import {} rows", size))?;

    tx.blocking_send(tuple)
        .map_err(|_| DatabaseError::InternalError(format!("ChannelClose")))?;
    Ok(())
}

impl CopyFromFile {
    /// Read records from file using blocking IO.
    ///
    /// The read data chunks will be sent through `tx`.
    fn read_file_blocking(mut self, tx: Sender<Tuple>) -> Result<()> {
        let file = File::open(self.op.source.path)?;
        let mut buf_reader = BufReader::new(file);
        let mut reader = match self.op.source.format {
            FileFormat::Csv {
                delimiter,
                quote,
                escape,
                header,
            } => csv::ReaderBuilder::new()
                .delimiter(delimiter as u8)
                .quote(quote as u8)
                .escape(escape.map(|c| c as u8))
                .has_headers(header)
                .from_reader(&mut buf_reader),
        };

        let column_count = self.op.schema_ref.len();
        let types = self
            .op
            .schema_ref
            .iter()
            .map(|column| column.datatype().clone())
            .collect_vec();
        let tuple_builder = TupleBuilder::new(types, self.op.schema_ref.clone());

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
            tx.blocking_send(tuple_builder.build_with_row(record.iter())?)
                .map_err(|_| DatabaseError::InternalError(format!("ChannelClose")))?;
        }
        Ok(())
    }
}
