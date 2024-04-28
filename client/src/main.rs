use anyhow::Result;
use client::{create_table, SQLClient};
use rustyline::error::ReadlineError;
use rustyline::DefaultEditor;

#[tokio::main]
async fn main() -> Result<()> {
    // `()` can be used when no completer is required
    let mut rl = DefaultEditor::new()?;
    println!("Ctrl-C to exit, .exit to exit, .help to get help");
    if rl.load_history("history.txt").is_err() {
        println!("No previous history.");
    }
    let client = SQLClient::connect().await?;
    loop {
        let readline = rl.readline("SQL> ");
        match readline {
            Ok(line) => {
                rl.add_history_entry(line.as_str())?;
                match line.as_str() {
                    ".exit" => break,
                    ".help"|".h"=>{
                        println!("show tables: show all tables");
                        println!("describe [table]: describe table schema");
                        println!("explain [sql]: explain sql");
                        println!("type Postgre dilect SQL to query");
                    }
                    _=>match client.query(line.as_str()).await {
                        Ok(result) => println!("{}", create_table(result)),
                        Err(e) => {
                            println!("Error: {:?}", e);
                        }
                    }
                }
                // match client.query(line.as_str()).await {
                //     Ok(result) => println!("{}", create_table(result)),
                //     Err(e) => {
                //         println!("Error: {:?}", e);
                //     }
                // }
                // let result = client.query(line.as_str()).await?;
                // println!("{}", create_table(result));
            }
            Err(ReadlineError::Interrupted) => {
                println!("exit by Ctrl-C");
                break;
            }
            Err(ReadlineError::Eof) => {
                println!("exit by Ctrl-D");
                break;
            }
            Err(err) => {
                println!("Error: {:?}", err);
            }
        }
    }
    rl.save_history("history.txt")?;
    Ok(())
}
