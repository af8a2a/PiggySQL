use criterion::{criterion_group, criterion_main, Criterion};
use piggysql::{
    db::Database,
    errors::*,
    storage::{piggy_stroage::PiggyKVStroage},
};

async fn data_source_lsm() -> Result<Database<PiggyKVStroage>> {
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

pub async fn lsm_benchmark_100000(engine: &Database<PiggyKVStroage>) -> Result<()> {
    let _ = engine
        .run("SELECT * FROM benchtable where id=490000")
        .await?;
    Ok(())
}
pub async fn lsm_without_primary_benchmark_100000(engine: &Database<PiggyKVStroage>) -> Result<()> {
    let _ = engine
        .run("SELECT * FROM benchtable where val=490000")
        .await?;
    Ok(())
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

criterion_group!(
    name = benches;
    config = Criterion::default();
    targets = lsm_benchmark
);
criterion_main!(benches);
