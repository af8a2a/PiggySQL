use criterion::{criterion_group, criterion_main, Criterion};
use piggysql::{
    db::Database,
    errors::*,
    storage::{
        engine::{
            bitcask::BitCask,
            lsm::{lsm_storage::LsmStorageOptions, LSM},
            memory::Memory,
            sled_store::SledStore,
        }, experiment::LSM, MVCCLayer
    },
};

async fn data_source() -> Result<Database<MVCCLayer<Memory>>> {
    let db = Database::new(MVCCLayer::new(Memory::new()))?;

    db.run(
        "CREATE TABLE benchtable(
            id INT PRIMARY KEY,
            val INT);
            ",
    )
    .await?;
    let mut batch = String::new();
    for i in 0..500000 {
        batch += format!("({},{})", i, i).as_str();
        batch += ","
    }
    batch += format!("({},{})", 500001, 500001).as_str();

    db.run(&format!("INSERT INTO benchtable VALUES {}", batch))
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
        "CREATE TABLE benchtable(
            id INT PRIMARY KEY,
            val INT);
            ",
    )
    .await?;
    let mut batch = String::new();
    for i in 0..500000 {
        batch += format!("({},{})", i, i).as_str();
        batch += ","
    }
    batch += format!("({},{})", 500001, 500001).as_str();

    db.run(&format!("INSERT INTO benchtable VALUES {}", batch))
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
        "CREATE TABLE benchtable(
            id INT PRIMARY KEY,
            val INT);
            ",
    )
    .await?;
    let mut batch = String::new();
    for i in 0..500000 {
        batch += format!("({},{})", i, i).as_str();
        batch += ","
    }
    batch += format!("({},{})", 500001, 500001).as_str();

    db.run(&format!("INSERT INTO benchtable VALUES {}", batch))
        .await?;
    Ok(db)
}
async fn data_source_lsm() -> Result<Database<LSM>> {
    let path = tempdir::TempDir::new("piggydb").unwrap().path().join("lsm");
    let db = Database::new_lsm(path)?;
    db.run(
        "CREATE TABLE benchtable(
            id INT PRIMARY KEY,
            val INT);
            ",
    )
    .await?;
    let mut batch = String::new();
    for i in 0..500000 {
        batch += format!("({},{})", i, i).as_str();
        batch += ","
    }
    batch += format!("({},{})", 500001, 500001).as_str();

    db.run(&format!("INSERT INTO benchtable VALUES {}", batch))
        .await?;
    Ok(db)
}
pub async fn primary_key_benchmark_100000(engine: &Database<MVCCLayer<Memory>>) -> Result<()> {
    let _ = engine
        .run("SELECT * FROM benchtable where id=490000")
        .await?;
    Ok(())
}
pub async fn without_primary_key_benchmark_100000(
    engine: &Database<MVCCLayer<Memory>>,
) -> Result<()> {
    let _ = engine
        .run("SELECT * FROM benchtable where val=490000")
        .await?;
    Ok(())
}

pub async fn bitcask_benchmark_100000(engine: &Database<MVCCLayer<BitCask>>) -> Result<()> {
    let _ = engine
        .run("SELECT * FROM benchtable where id=490000")
        .await?;
    Ok(())
}
pub async fn bitcask_without_primary_benchmark_100000(
    engine: &Database<MVCCLayer<BitCask>>,
) -> Result<()> {
    let _ = engine
        .run("SELECT * FROM benchtable where val=490000")
        .await?;
    Ok(())
}
pub async fn sled_benchmark_100000(engine: &Database<MVCCLayer<SledStore>>) -> Result<()> {
    let _ = engine
        .run("SELECT * FROM benchtable where id=490000")
        .await?;
    Ok(())
}
pub async fn sled_without_primary_benchmark_100000(
    engine: &Database<MVCCLayer<SledStore>>,
) -> Result<()> {
    let _ = engine
        .run("SELECT * FROM benchtable where val=490000")
        .await?;
    Ok(())
}
pub async fn lsm_benchmark_100000(engine: &Database<LSM>) -> Result<()> {
    let _ = engine
        .run("SELECT * FROM benchtable where id=490000")
        .await?;
    Ok(())
}
pub async fn lsm_without_primary_benchmark_100000(engine: &Database<LSM>) -> Result<()> {
    let _ = engine
        .run("SELECT * FROM benchtable where val=490000")
        .await?;
    Ok(())
}
fn memory_benchmark(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(6)
        .enable_all()
        .build()
        .unwrap();
    let engine = rt.block_on(async { data_source().await.unwrap() });
    c.bench_function("select rows with primary key", |b| {
        b.to_async(&rt)
            .iter(|| async { primary_key_benchmark_100000(&engine).await })
    });
    c.bench_function("select rows without primary key", |b| {
        b.to_async(&rt)
            .iter(|| async { without_primary_key_benchmark_100000(&engine).await })
    });
}
fn bitcask_benchmark(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(6)
        .enable_all()
        .build()
        .unwrap();
    let bitcask = rt.block_on(async { data_source_bitcask().await.unwrap() });
    c.bench_function("bitcask benchmark select rows with primary key", |b| {
        b.to_async(&rt)
            .iter(|| async { bitcask_benchmark_100000(&bitcask).await })
    });
    c.bench_function("bitcask benchmark select rows without primary key", |b| {
        b.to_async(&rt)
            .iter(|| async { bitcask_without_primary_benchmark_100000(&bitcask).await })
    });
}
fn lsm_benchmark(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(6)
        .enable_all()
        .build()
        .unwrap();
    let lsm = rt.block_on(async { data_source_lsm().await.unwrap() });
    c.bench_function("lsm benchmark select rows with primary key", |b| {
        b.to_async(&rt)
            .iter(|| async { lsm_benchmark_100000(&lsm).await.unwrap() })
    });
    c.bench_function("lsm benchmark select rows without primary key", |b| {
        b.to_async(&rt)
            .iter(|| async { lsm_without_primary_benchmark_100000(&lsm).await.unwrap() })
    });
}

fn sled_benchmark(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(6)
        .enable_all()
        .build()
        .unwrap();

    let sled = rt.block_on(async { data_source_sled().await.unwrap() });

    c.bench_function("sled benchmark select rows with primary key", |b| {
        b.to_async(&rt)
            .iter(|| async { sled_benchmark_100000(&sled).await })
    });
    c.bench_function("sled benchmark select rows without primary key", |b| {
        b.to_async(&rt)
            .iter(|| async { sled_without_primary_benchmark_100000(&sled).await })
    });
}
criterion_group!(
    name = benches;
    config = Criterion::default();
    targets = lsm_benchmark
);
criterion_main!(benches);
