use criterion::{criterion_group, criterion_main, Criterion};
use piggysql::{
    db::Database,
    storage::{engine::memory::Memory, MVCCLayer},
};

use anyhow::Result;

async fn data_source() -> Result<Database<MVCCLayer<Memory>>> {
    let db = Database::new(MVCCLayer::new(Memory::new()))?;

    db.run(
        "CREATE TABLE BenchTable(
            id INT PRIMARY KEY,
            val INT);
            ",
    )
    .await?;
    for i in 0..100000 {
        db.run(&format!("INSERT INTO BenchTable VALUES ({},{})", i, i,))
            .await
            .expect("insert error");
    }

    Ok(db)
}

pub async fn primary_key_benchmark_100000(engine: &Database<MVCCLayer<Memory>>) -> Result<()> {
    let _ = engine
        .run("SELECT * FROM BenchTable where id>90000")
        .await?;
    Ok(())
}
pub async fn without_primary_key_benchmark_100000(
    engine: &Database<MVCCLayer<Memory>>,
) -> Result<()> {
    let _ = engine
        .run("SELECT * FROM BenchTable where val>90000")
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

    c.bench_function("select rows with primary key", |b| {
        b.to_async(&rt)
            .iter(|| primary_key_benchmark_100000(&engine))
    });
    c.bench_function("select rows without primary key", |b| {
        b.to_async(&rt)
            .iter(|| without_primary_key_benchmark_100000(&engine))
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
