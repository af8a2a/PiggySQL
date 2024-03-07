use std::{collections::HashMap, path::PathBuf};

use config::Config;
use piggysql::{
    server::Server,
    storage::engine::{bitcask::BitCask, memory::Memory, sled_store::SledStore},
};
use tracing::Level;
use tracing_subscriber::FmtSubscriber;

#[tokio::main(worker_threads = 8)]
async fn main() {
    let subscriber = FmtSubscriber::builder()
        // all spans/events with a level higher than TRACE (e.g, debug, info, warn, etc.)
        // will be written to stdout.
        .with_max_level(Level::DEBUG)
        // completes the builder.
        .finish();
    tracing::subscriber::set_global_default(subscriber).expect("setting default subscriber failed");
    let settings = Config::builder()
        // Add in `./Settings.toml`
        .add_source(config::File::with_name("config/Settings"))
        // Add in settings from the environment (with a prefix of APP)
        // Eg.. `APP_DEBUG=1 ./target/app` would set the `debug` key
        // .add_source(config::Environment::with_prefix("APP"))
        .build()
        .unwrap();
    let setting_map = settings
        .try_deserialize::<HashMap<String, String>>()
        .unwrap();
    let filename = setting_map.get("filename").unwrap();
    let engine = setting_map.get("engine").unwrap();
    match engine.as_str() {
        "sled" => {
            let store = SledStore::new(PathBuf::from(filename)).unwrap();
            let server = Server::new(store).await.unwrap();
            Server::run(server).await;
        }
        "bitcask" => {
            let store = BitCask::new(PathBuf::from(filename)).unwrap();
            let server = Server::new(store).await.unwrap();
            Server::run(server).await;
        }
        _ => {
            //fallback
            let store = Memory::new();
            let server = Server::new(store).await.unwrap();
            Server::run(server).await;
        }
    };
}
