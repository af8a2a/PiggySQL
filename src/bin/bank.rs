use piggysql::client::SQLClient;
use piggysql::errors::*;

#[tokio::main]
async fn main() -> Result<()> {
    let setup = SQLClient::connect().await?;
    setup
        .query(
            "CREATE TABLE genres (
            id INTEGER PRIMARY KEY,
            name VARCHAR NOT NULL);",
        )
        .await?;
    setup
        .query(
            "CREATE TABLE studios (
            id INTEGER PRIMARY KEY,
            name VARCHAR NOT NULL
        );",
        )
        .await?;
    setup
        .query(
            "CREATE TABLE movies (
        id INTEGER PRIMARY KEY,
        title VARCHAR NOT NULL,
        studio_id INTEGER NOT NULL ,
        genre_id INTEGER NOT NULL ,
        released INTEGER NOT NULL,
        rating INTEGER
    );",
        )
        .await?;
    setup
        .query(
            "INSERT INTO genres VALUES
    (1, 'Science Fiction'),
    (2, 'Action'),
    (3, 'Drama'),
    (4, 'Comedy');
",
        )
        .await?;

    setup
        .query(
            "INSERT INTO studios VALUES
            (1, 'Mosfilm'),
            (2, 'Lionsgate'),
            (3, 'StudioCanal'),
            (4, 'Warner Bros'),
            (5, 'Focus Features');",
        )
        .await?;
    setup
        .query(
            "INSERT INTO movies VALUES
            (1,  'Stalker',             1, 1, 1979, 8),
            (2,  'Sicario',             2, 2, 2015, 7),
            (3,  'Primer',              3, 1, 2004, 6),
            (4,  'Heat',                4, 2, 1995, 8),
            (5,  'The Fountain',        4, 1, 2006, 7),
            (6,  'Solaris',             1, 1, 1972, 8),
            (7,  'Gravity',             4, 1, 2013, 7),
            (8,  '21 Grams',            5, 3, 2003, 7),
            (9,  'Birdman',             4, 4, 2014, 7),
            (10, 'Inception',           4, 1, 2010, 8),
            (11, 'Lost in Translation', 5, 4, 2003, 7),
            (12, 'Eternal Sunshine of the Spotless Mind', 5, 3, 2004, 8);",
        )
        .await?;
    tokio::time::sleep(std::time::Duration::from_secs(1)).await;
    // let mut sql = vec![];
    // sql.push(format!(
    //     "SELECT m.id, m.title, g.name FROM movies m JOIN genres g ON m.genre_id = g.id LIMIT 4;"
    // ));
    // sql.push(format!("UPDATE movies SET rating = rating+1;"));

    let mut tasks = vec![];

    for _ in 0..6 {
        tasks.push(tokio::spawn(tokio::spawn(async move {
            let mut sql = vec![];
            sql.push(
                "SELECT m.id, m.title, g.name FROM movies m JOIN genres g ON m.genre_id = g.id LIMIT 4;".to_string()
            );
            sql.push("UPDATE movies SET rating = rating+1;".to_string());
            let client = SQLClient::connect().await.unwrap();
            client.query_txn(sql).await.unwrap();
        })));
    }
    let mut outputs = Vec::with_capacity(tasks.len());
    for task in tasks {
        outputs.push(task.await.unwrap());
    }

    Ok(())
}
