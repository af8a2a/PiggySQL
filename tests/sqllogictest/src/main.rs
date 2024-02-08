use std::path::Path;

use sqllogictest::Runner;
use sqllogictest_test::Mock;
#[tokio::main]

async fn main() {
    const SLT_PATTERN: &str = "tests/slt/**/*.slt";

    let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("..").join("..");
    std::env::set_current_dir(path).unwrap();

    println!("PiggySQL Test Start!\n");

    for slt_file in glob::glob(SLT_PATTERN).expect("failed to find slt files") {
        let filepath = slt_file
            .expect("failed to read slt file")
            .to_str()
            .unwrap()
            .to_string();
        println!("-> Now the test file is: {}", filepath);

        let db = Mock::new();
        let mut tester = Runner::new(db);

        if let Err(err) = tester.run_file_async(filepath).await {
            
            panic!("test error: {}", err);
        }
        println!("-> Pass!\n\n")
    }
}
