use clap::Parser;
use std::io::BufRead;
use twprs::model::User;

fn main() -> Result<(), Error> {
    let opts: Opts = Opts::parse();

    let limit = opts.limit.unwrap_or(usize::MAX);

    let stdin = std::io::stdin();
    let mut users = stdin
        .lock()
        .lines()
        .map(|line| line.map_err(Error::from))
        .map(|line| {
            let line = line?;
            if line.starts_with('{') {
                Ok((None, serde_json::from_str::<User>(&line)?))
            } else {
                let (rank_str, json) = line.split_once(',').expect("Invalid line");
                let rank = rank_str.parse::<u64>().expect("Invalid rank");
                Ok((Some(rank), serde_json::from_str::<User>(&json)?))
            }
        })
        .collect::<Result<Vec<_>, Error>>()?;

    if opts.sort {
        users.sort_by_key(|(_, user)| (user.id, std::cmp::Reverse(user.snapshot)));
        users.dedup_by_key(|(_, user)| user.id);
        users.sort_by_key(|(_, user)| std::cmp::Reverse(user.followers_count));
    }

    users.truncate(limit);

    println!(
        r#"<table><tr><th></th><th align="left">Twitter ID</th><th align="left">Screen name</th>"#
    );
    print!(
        r#"<th align="left">Created</th><th align="left">Status</th><th align="left">Followers</th>"#
    );

    if let Some((Some(_), _)) = users.first() {
        print!(r#"<th align="left">Ranking</th>"#);
    }
    println!("</tr>");

    for (rank, user) in users {
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
            status.push('🔒');
        }
        if user.verified {
            status.push_str("✔️");
        }

        print!(
            "<tr><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td align=\"center\">{}</td><td>{}</td>",
            img,
            id_link,
            screen_name_link,
            created_at,
            status,
            user.followers_count
        );

        if let Some(rank) = rank {
            print!("<td>{}</td>", rank);
        }

        println!("</tr>");
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
    #[clap(long)]
    sort: bool,
    #[clap(long)]
    limit: Option<usize>,
}
