use chrono::{Date, NaiveDate, TimeZone, Utc};
use clap::Parser;
use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::Path;
use twprs::model::User;

fn main() -> Result<(), Error> {
    let opts: Opts = Opts::parse();

    match opts.command {
        Command::Create { input, output } => {
            let path = Path::new(&input);
            let output_file = File::create(output)?;
            let mut writer = twprs::avro::writer(output_file);

            if path.is_file() {
                let reader = BufReader::new(File::open(path)?);

                for line in reader.lines() {
                    let user = serde_json::from_str::<User>(&line?)?;
                    writer.append_ser(user)?;
                }
            } else if path.is_dir() {
                for entry in std::fs::read_dir(path)? {
                    let entry = entry?;
                    eprintln!("Reading file: {:?}", entry.path());
                    let reader = BufReader::new(File::open(entry.path())?);

                    for (i, line) in reader.lines().enumerate() {
                        let user = match serde_json::from_str::<User>(&line?) {
                            Ok(value) => value,
                            Err(error) => {
                                panic!("At {}: {:?}", i, error);
                            }
                        };
                        writer.append_ser(user)?;
                    }
                }
            }
        }
    }

    Ok(())
}

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("I/O error")]
    Io(#[from] std::io::Error),
    #[error("Avro error")]
    Avro(#[from] apache_avro::Error),
    #[error("JSON error")]
    Json(#[from] serde_json::Error),
}

#[derive(Debug, Parser)]
#[clap(name = "avro", version, author)]
struct Opts {
    /// Level of verbosity
    #[clap(short, long, parse(from_occurrences))]
    verbose: i32,
    #[clap(subcommand)]
    command: Command,
}

#[derive(Debug, Parser)]
enum Command {
    Create {
        /// Input path
        #[clap(short, long)]
        input: String,
        /// Output path
        #[clap(short, long)]
        output: String,
    },
}
