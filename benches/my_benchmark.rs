use criterion::{criterion_group, criterion_main, Criterion};
use piggysql::{
    db::Database,
    errors::*,
    storage::{
        engine::{lsm::BitCask, memory::Memory, sled_store::SledStore},
        MVCCLayer,
    },
};
use tempfile::{tempdir, tempdir_in};

async fn data_source() -> Result<Database<MVCCLayer<Memory>>> {
    let db = Database::new(MVCCLayer::new(Memory::new()))?;

    db.run(
        "CREATE TABLE BenchTable(
            id INT PRIMARY KEY,
            val INT);
            ",
    )
    .await?;
    let mut batch = String::new();
    for i in 0..100000 {
        batch += format!("({},{})", i, i).as_str();
        batch += ","
    }
    batch += format!("({},{})", 100001, 100001).as_str();

    db.run(&format!("INSERT INTO BenchTable VALUES {}", batch))
        .await?;
    Ok(db)
}
async fn data_source_bitcask() -> Result<Database<MVCCLayer<BitCask>>> {
    let path = tempdir::TempDir::new("piggydb")
        .unwrap()
        .path()
        .join("piggydb");
    let db = Database::new(MVCCLayer::new(BitCask::new(path)?))?;

    db.run(
        "CREATE TABLE BenchTable(
            id INT PRIMARY KEY,
            val INT);
            ",
    )
    .await?;
    let mut batch = String::new();
    for i in 0..100000 {
        batch += format!("({},{})", i, i).as_str();
        batch += ","
    }
    batch += format!("({},{})", 100001, 100001).as_str();

    db.run(&format!("INSERT INTO BenchTable VALUES {}", batch))
        .await?;
    Ok(db)
}
async fn data_source_sled() -> Result<Database<MVCCLayer<SledStore>>> {
    let path = tempdir::TempDir::new("piggydb")
        .unwrap()
        .path()
        .join("piggydb");
    let db = Database::new(MVCCLayer::new(SledStore::new(path)?))?;

    db.run(
        "CREATE TABLE BenchTable(
            id INT PRIMARY KEY,
            val INT);
            ",
    )
    .await?;
    let mut batch = String::new();
    for i in 0..100000 {
        batch += format!("({},{})", i, i).as_str();
        batch += ","
    }
    batch += format!("({},{})", 100001, 100001).as_str();

    db.run(&format!("INSERT INTO BenchTable VALUES {}", batch))
        .await?;
    Ok(db)
}
pub async fn primary_key_benchmark_100000(engine: &Database<MVCCLayer<Memory>>) -> Result<()> {
    let _ = engine
        .run("SELECT * FROM BenchTable where id=90000")
        .await?;
    Ok(())
}
pub async fn without_primary_key_benchmark_100000(
    engine: &Database<MVCCLayer<Memory>>,
) -> Result<()> {
    let _ = engine
        .run("SELECT * FROM BenchTable where val=90000")
        .await?;
    Ok(())
}
pub async fn si_key_benchmark_100000(engine: &Database<MVCCLayer<Memory>>) -> Result<()> {
    let mut txn = engine.new_transaction().await?;
    let _ = txn.run("SELECT * FROM BenchTable where val=90000").await?;
    Ok(())
}

pub async fn ssi_key_benchmark_100000(engine: &Database<MVCCLayer<Memory>>) -> Result<()> {
    let mut txn = engine.new_transaction().await?;
    txn.run("set isolation = serializable").await?;
    let _ = txn.run("SELECT * FROM BenchTable where val=90000").await?;
    Ok(())
}

pub async fn bitcask_benchmark_100000(engine: &Database<MVCCLayer<BitCask>>) -> Result<()> {
    let _ = engine
        .run("SELECT * FROM BenchTable where val=90000")
        .await?;
    Ok(())
}
pub async fn sled_benchmark_100000(engine: &Database<MVCCLayer<SledStore>>) -> Result<()> {
    let _ = engine
        .run("SELECT * FROM BenchTable where val=90000")
        .await?;
    Ok(())
}
fn criterion_benchmark(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(8)
        .enable_all()
        .build()
        .unwrap();
    let engine = rt.block_on(async { data_source().await.unwrap() });
    let bitcask = rt.block_on(async { data_source_bitcask().await.unwrap() });
    let sled=rt.block_on(async { data_source_sled().await.unwrap() });
    c.bench_function("select rows with primary key", |b| {
        b.to_async(&rt)
            .iter(|| async { primary_key_benchmark_100000(&engine).await })
    });
    c.bench_function("select rows without primary key", |b| {
        b.to_async(&rt)
            .iter(|| async { without_primary_key_benchmark_100000(&engine).await })
    });
    c.bench_function("transaction in Snapshot isolation", |b| {
        b.to_async(&rt)
            .iter(|| async { si_key_benchmark_100000(&engine).await })
    });
    c.bench_function("transaction in Serializable Snapshot isolation", |b| {
        b.to_async(&rt)
            .iter(|| async { ssi_key_benchmark_100000(&engine).await })
    });
    c.bench_function("bitcask benchmark select rows with primary key", |b| {
        b.to_async(&rt)
            .iter(|| async { bitcask_benchmark_100000(&bitcask).await })
    });
    c.bench_function("sled benchmark select rows with primary key", |b| {
        b.to_async(&rt)
            .iter(|| async { sled_benchmark_100000(&sled).await })
    });

}
criterion_group!(
    name = benches;
    config = Criterion::default()
    .sample_size(10)
    .measurement_time(std::time::Duration::from_secs(30))
    ;
    targets = criterion_benchmark
);
criterion_main!(benches);
