use clap::Parser;
use std::fs::File;
use twprs::model::User;
use twprs_db::db::ProfileDb;

fn main() -> Result<(), Error> {
    let opts: Opts = Opts::parse();
    let _ = twprs::cli::init_logging(opts.verbose)?;
    let db = ProfileDb::open(opts.db, false)?;

    match opts.command {
        Command::Import { input } => {
            let file = File::open(input)?;
            let reader = twprs::avro::reader(file)?;

            for value in reader {
                let user = apache_avro::from_value::<User>(&value?)?;
                db.update(&user)?;
            }
        }
        Command::Lookup { id } => {
            let users = db.lookup(id)?;

            for user in users {
                println!("{}", serde_json::to_value(user)?);
            }
        }
    }

    Ok(())
}

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("ProfileDb error")]
    ProfileDb(#[from] twprs_db::db::Error),
    #[error("Profile Avro error")]
    ProfileAvro(#[from] twprs::avro::Error),
    #[error("Avro decoding error")]
    Avro(#[from] apache_avro::Error),
    #[error("JSON encoding error")]
    Json(#[from] serde_json::Error),
    #[error("I/O error")]
    Io(#[from] std::io::Error),
    #[error("Log initialization error")]
    LogInitialization(#[from] log::SetLoggerError),
}

#[derive(Debug, Parser)]
#[clap(name = "profiles", version, author)]
struct Opts {
    /// Level of verbosity
    #[clap(short, long, parse(from_occurrences))]
    verbose: i32,
    /// Database path
    #[clap(long)]
    db: String,
    #[clap(subcommand)]
    command: Command,
}

#[derive(Debug, Parser)]
enum Command {
    Import {
        /// Avro input path
        #[clap(short, long)]
        input: String,
    },
    Lookup {
        /// Twitter user ID
        #[clap(long)]
        id: u64,
    },
}
