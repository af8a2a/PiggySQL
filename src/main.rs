use piggysql::server::Server;

#[tokio::main(worker_threads = 8)]
async fn main() {

    Server::run().await;

}

