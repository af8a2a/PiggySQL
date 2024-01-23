use crate::{error::Result, types::Key};
use futures::Stream;
use std::pin::Pin;

use self::data_row::DataRow;

mod alter_table;
mod data_row;
mod index;
mod transaction;

pub type RowIter = Pin<Box<dyn Stream<Item = Result<(Key, DataRow)>>>>;
