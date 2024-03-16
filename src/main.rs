use std::path::PathBuf;

use piggysql::{
    server::Server,
    storage::engine::{
        bitcask::BitCask,
        lsm::{lsm_storage::LsmStorageOptions, LSM},
        memory::Memory,
        sled_store::SledStore,
    },
    CONFIG_MAP,
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
    let filename = CONFIG_MAP.get("filename").unwrap();
    let engine = CONFIG_MAP.get("engine").unwrap();
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
        "lsm" => {
            let bloom_enable = CONFIG_MAP
                .get("bloom_filter")
                .unwrap()
                .parse::<bool>()
                .expect("bloom_filter must be true or false");
            let bloom_false_positive_rate = CONFIG_MAP
                .get("bloom_false_positive_rate")
                .cloned()
                .unwrap_or("0.01".to_string())
                .parse::<f64>()
                .unwrap_or(0.01);

            let option = LsmStorageOptions::default()
                .with_enable_bloom(bloom_enable)
                .with_bloom_false_positive_rate(bloom_false_positive_rate);
            let store = LSM::new(PathBuf::from(filename), option);
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
