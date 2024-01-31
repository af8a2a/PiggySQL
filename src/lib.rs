#![feature(error_generic_member_access)]
#![allow(unused_doc_comments)]
#![feature(result_flattening)]
#![feature(coroutines)]
#![feature(iterator_try_collect)]
#![feature(slice_pattern)]
#![feature(bound_map)]
pub mod parser;
pub mod common;
pub mod storage;
pub mod binder;
pub mod result;
pub mod types;
pub mod expression;
pub mod planner;
pub mod catalog;
pub mod execution;
pub mod db;