use clap::Parser;
use egg_mode_extras::Client;
use std::collections::HashMap;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::Path;
use std::sync::Arc;
use twprs_bouncer::{
    gh::Repo,
    report::{self, Report},
};
use twprs_db::db::ProfileDb;

#[tokio::main]
async fn main() -> Result<(), Error> {
    let opts: Opts = Opts::parse();
    twprs::cli::init_logging(opts.verbose)?;

    match opts.command {
        Command::Gh { token } => {
            let repo = Repo::new("travisbrown", "bouncer", token).unwrap();

            let screen_name = repo.next_issue().await.unwrap();
            println!("{:?}", screen_name);
        }
        Command::CheckUser {
            token,
            db,
            bad,
            reload,
            screen_name,
        } => {
            let db = ProfileDb::open(db, false).map_err(report::Error::from)?;
            let client = Arc::new(
                Client::from_config_file("keys.toml")
                    .await
                    .map_err(report::Error::from)?,
            );
            let mut report = if reload {
                Report::load(client, "reports", screen_name).await?
            } else {
                Report::new(client, "reports", screen_name).await?
            };

            log::info!("Finding {} total users", report.total_user_count());

            let read_count = report.read_users(&db, None)?;
            log::info!(
                "Read {} cached users, downloading {}",
                read_count,
                report.missing_user_count()
            );

            let downloaded_count = report.download_missing_users().await?;
            log::info!(
                "Downloaded {} users, {} still missing",
                downloaded_count,
                report.missing_user_count()
            );

            let bad_users = read_bad_users(bad)?;
            report.save(&bad_users)?;
        }
    }

    Ok(())
}

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Report error")]
    Report(#[from] twprs_bouncer::report::Error),
    #[error("GitHub client error")]
    Gh(#[from] twprs_bouncer::gh::Error),
    #[error("Log initialization error")]
    LogInitialization(#[from] log::SetLoggerError),
}

#[derive(Debug, Parser)]
#[clap(name = "bouncer", version, author)]
struct Opts {
    /// Level of verbosity
    #[clap(short, long, parse(from_occurrences))]
    verbose: i32,
    #[clap(subcommand)]
    command: Command,
}

#[derive(Debug, Parser)]
enum Command {
    CheckUser {
        /// A GitHub personal access token (not needed for all operations)
        #[clap(short, long)]
        token: Option<String>,
        /// Database path
        #[clap(long)]
        db: String,
        /// Bad account list path
        #[clap(long)]
        bad: String,
        /// Only reload (don't re-run)
        #[clap(long)]
        reload: bool,
        /// Screen name of user to check
        screen_name: String,
    },
    Gh {
        /// A GitHub personal access token (not needed for all operations)
        #[clap(short, long)]
        token: Option<String>,
    },
}

fn read_bad_users<P: AsRef<Path>>(path: P) -> Result<HashMap<u64, usize>, report::Error> {
    BufReader::new(File::open(path)?)
        .lines()
        .enumerate()
        .map(|(index, line)| {
            let line = line?;
            let user_id = line
                .split(',')
                .next()
                .and_then(|field| field.parse::<u64>().ok())
                .ok_or_else(|| report::Error::UnexpectedCsvLine(line.clone()))?;

            Ok((user_id, index))
        })
        .collect()
}
