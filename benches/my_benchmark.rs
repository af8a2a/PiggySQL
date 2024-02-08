use criterion::{criterion_group, criterion_main, Criterion};
use piggysql::{
    db::Database,
    storage::{engine::memory::Memory, MVCCLayer},
};

// fn generate_random_string(length: usize) -> String {
//     let rng = thread_rng();
//     let mut random_string: Vec<u8> = rng.sample_iter(&Alphanumeric).take(length).collect();
//     String::from_utf8(random_string).expect("error")
// }
lazy_static! {
    static ref DATA: Database<MVCCLayer<Memory>> = data_source().unwrap();
}
use anyhow::Result;
use lazy_static::lazy_static;

fn data_source() -> Result<Database<MVCCLayer<Memory>>> {
    let db = Database::new(MVCCLayer::new(Memory::new()))?;
    db.run(
        "CREATE TABLE BenchTable(
        id INT PRIMARY KEY,
        val INT);
        ",
    )?;
    for i in 0..100000 {
        db.run(&format!("INSERT INTO BenchTable VALUES ({},{})", i, i,))
            .expect("insert error");
    }
    Ok(db)
}


pub fn primary_key_benchmark_100000() {
    let engine = &DATA;
    
    let _ = engine
        .run("SELECT * FROM BenchTable where id>50000")
        .unwrap();
}
pub fn without_primary_key_benchmark_100000() {
    let engine = &DATA;

    let _ = engine
        .run("SELECT * FROM BenchTable where val>50000")
        .unwrap();
}
fn criterion_benchmark(c: &mut Criterion) {
    c.bench_function("select in 1000 rows with primary key", |b| {
        b.iter(|| primary_key_benchmark_100000())
    });
    c.bench_function("select in 1000 rows without primary key", |b| {
        b.iter(|| without_primary_key_benchmark_100000())
    });
}
criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
