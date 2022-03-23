use chrono::{TimeZone, Utc};
use egg_mode_extras::{client::TokenType, Client};
use std::io::{BufRead, BufReader};
use twprs_scraper::{
    queue::UserQueue,
    scraper::{Error, Scraper},
};

#[tokio::main]
async fn main() -> Result<(), Error> {
    let stdin = std::io::stdin();
    let user_ids = stdin.lock().lines().map(|line| {
        let line = line?;
        let parts = line.split(",").collect::<Vec<_>>();

        let result: Result<_, Error> = Ok((
            parts[0].parse::<u64>().unwrap(),
            parts[1].parse::<u32>().unwrap(),
            Utc.timestamp(parts[2].parse::<i64>().unwrap(), 0),
        ));

        result
    });

    let queue = UserQueue::new(user_ids)?;
    let client = Client::from_config_file("keys.toml").await?;
    let scraper = Scraper::new(client, queue, 200);

    scraper.run().await;

    Ok(())
}
