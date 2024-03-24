pub mod binder;
pub mod catalog;
pub mod db;
pub mod errors;
pub mod execution;
pub mod expression;
pub mod optimizer;
pub mod parser;
pub mod planner;
pub mod server;
pub mod storage;
pub mod types;


use lazy_static::lazy_static;
lazy_static! {
    pub static ref CONFIG_MAP: std::collections::HashMap<String, String> = config::Config::builder()
        .add_source(config::File::with_name("config/Settings.toml"))
        .build()
        .unwrap()
        .try_deserialize()
        .unwrap();

}