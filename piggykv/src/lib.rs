#![feature(fs_try_exists)]
#![feature(cursor_remaining)]
#![feature(slice_pattern)]
#![feature(is_sorted)]
use error::KernelError;

pub mod io;
pub mod lsm;
pub mod error;
pub mod utils;


pub type KernelResult<T> = std::result::Result<T, KernelError>;
