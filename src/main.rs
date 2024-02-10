
use std::{io, sync::Arc};

use clap::{command, Parser};
use pgwire::{api::{auth::{noop::NoopStartupHandler, StartupHandler}, query::{ExtendedQueryHandler, PlaceholderExtendedQueryHandler, SimpleQueryHandler}, MakeHandler, StatelessMakeHandler}, tokio::process_socket};
use piggysql::server::{Args, Server};
use tokio::net::TcpListener;



async fn quit() -> io::Result<()> {
    #[cfg(unix)]
    {
        let mut interrupt =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt())?;
        let mut terminate =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
        tokio::select! {
            _ = interrupt.recv() => (),
            _ = terminate.recv() => (),
        }
        Ok(())
    }
    #[cfg(windows)]
    {
        let mut signal = tokio::signal::windows::ctrl_c()?;
        let _ = signal.recv().await;

        Ok(())
    }
}


#[tokio::main(worker_threads = 8)]
async fn main() {


    let args = Args::parse();

    let backend = Server::new().await.unwrap();
    let processor = Arc::new(backend);
    // We have not implemented extended query in this server, use placeholder instead
    let placeholder = Arc::new(StatelessMakeHandler::new(Arc::new(
        PlaceholderExtendedQueryHandler,
    )));
    let authenticator = Arc::new(StatelessMakeHandler::new(Arc::new(NoopStartupHandler)));
    let server_addr = format!("{}:{}", args.ip, args.port);
    let listener = TcpListener::bind(server_addr).await.unwrap();

    tokio::select! {
        res = server_run(processor, placeholder, authenticator, listener) => {
            if let Err(err) = res {
            }
        }
        _ = quit() => {}
    }
}

async fn server_run<
    A: MakeHandler<Handler = Arc<impl StartupHandler + 'static>>,
    Q: MakeHandler<Handler = Arc<impl SimpleQueryHandler + 'static>>,
    EQ: MakeHandler<Handler = Arc<impl ExtendedQueryHandler + 'static>>,
>(
    processor: Arc<Q>,
    placeholder: Arc<EQ>,
    authenticator: Arc<A>,
    listener: TcpListener,
) -> io::Result<()> {
    loop {
        let incoming_socket = listener.accept().await?;
        let authenticator_ref = authenticator.make();
        let processor_ref = processor.make();
        let placeholder_ref = placeholder.make();

        tokio::spawn(async move {
            if let Err(err) = process_socket(
                incoming_socket.0,
                None,
                authenticator_ref,
                processor_ref,
                placeholder_ref,
            )
            .await
            {
                {}
            }
        });
    }
}
