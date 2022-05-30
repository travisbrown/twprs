use clap::Parser;
use flate2::read::GzDecoder;
use std::collections::HashSet;
use std::fs::File;
use std::io::{BufRead, BufReader, Write};
use std::path::Path;
use twprs::model::User;

fn main() -> Result<(), Error> {
    let opts: Opts = Opts::parse();

    let stdin = std::io::stdin();
    let mut users = stdin
        .lock()
        .lines()
        .map(|line| line.map_err(Error::from))
        .map(|line| {
            let line = line?;
            Ok(serde_json::from_str::<User>(&line)?)
        })
        .collect::<Result<Vec<_>, Error>>()?;

    users.sort_by_key(|user| std::cmp::Reverse(user.followers_count));

    println!(
        r#"<table><tr><th></th><th align="left">Twitter ID</th><th align="left">Screen name</th>"#
    );
    println!(
        r#"<th align="left">Created</th><th align="left">Status</th><th align="left">Follower count</th></tr>"#
    );

    for user in users {
        let img = format!(
            "<a href=\"{}\"><img src=\"{}\" width=\"40px\" height=\"40px\" align=\"center\"/></a>",
            user.profile_image_url_https, user.profile_image_url_https
        );
        let id_link = format!(
            "<a href=\"https://twitter.com/intent/user?user_id={}\">{}</a>",
            user.id, user.id
        );
        let screen_name_link = format!(
            "<a href=\"https://twitter.com/{}\">{}</a>",
            user.screen_name, user.screen_name
        );

        let created_at = user.created_at()?.format("%Y-%m-%d");

        let mut status = String::new();
        if user.protected {
            status.push_str("🔒");
        }
        if user.verified {
            status.push_str("✔️");
        }

        println!(
            "<tr><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td align=\"center\">{}</td><td>{}</td></tr>",
            img,
            id_link,
            screen_name_link,
            created_at,
            status,
            user.followers_count
        );
    }

    println!(r#"</table>"#);

    Ok(())
}

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("I/O error")]
    Io(#[from] std::io::Error),
    #[error("Date format error")]
    ChronoParse(#[from] chrono::format::ParseError),
    #[error("JSON error")]
    Json(#[from] serde_json::Error),
}

#[derive(Debug, Parser)]
#[clap(name = "avro", version, author)]
struct Opts {
    /// Level of verbosity
    #[clap(short, long, parse(from_occurrences))]
    verbose: i32,
}
