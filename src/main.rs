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
use tracing_appender::{non_blocking, rolling};
use tracing_subscriber::FmtSubscriber;

#[tokio::main(worker_threads = 8)]
async fn main() {
    let log_level = CONFIG_MAP.get("log_level").unwrap();
    let log_level = match log_level.to_lowercase().as_str() {
        "info" => Level::INFO,
        "debug" => Level::DEBUG,
        "error" => Level::ERROR,
        "trace" => Level::TRACE,
        _ => Level::INFO,
    };
    let file_appender = tracing_appender::rolling::hourly("./", "db.log");
    let (non_blocking, _guard) = tracing_appender::non_blocking(file_appender);

    let subscriber = FmtSubscriber::builder()
        // all spans/events with a level higher than TRACE (e.g, debug, info, warn, etc.)
        // will be written to stdout.
        .with_max_level(log_level)
        .with_writer(non_blocking)
        // completes the builder.
        .finish();

    tracing::subscriber::set_global_default(subscriber).expect("setting default subscriber failed");
    let filename = CONFIG_MAP.get("filename").unwrap();
    let engine = CONFIG_MAP.get("engine").unwrap();
    match engine.to_lowercase().as_str() {
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
            let bloom_false_positive_rate = CONFIG_MAP
                .get("bloom_false_positive_rate")
                .cloned()
                .unwrap_or("0.01".to_string())
                .parse::<f64>()
                .unwrap_or(0.01);
            let compaction = CONFIG_MAP.get("compaction").unwrap().clone();
            let option = match compaction.to_lowercase().as_str() {
                "leveled" => LsmStorageOptions::leveled_compaction()
                    .with_bloom_false_positive_rate(bloom_false_positive_rate),
                "simple" => LsmStorageOptions::default()
                    .with_bloom_false_positive_rate(bloom_false_positive_rate),
                _ => LsmStorageOptions::no_compaction()
                    .with_bloom_false_positive_rate(bloom_false_positive_rate),
            };
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
