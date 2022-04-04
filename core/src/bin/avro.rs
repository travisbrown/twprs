use clap::Parser;
use flate2::read::GzDecoder;
use std::fs::File;
use std::io::{BufRead, BufReader, Write};
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
                write_from_path(path, &mut writer)?;
            } else if path.is_dir() {
                for entry in std::fs::read_dir(path)? {
                    write_from_path(entry?.path(), &mut writer)?;
                }
            }

            writer.flush()?;
        }
        Command::Dump { input } => {
            let file = File::open(input)?;
            let reader = twprs::avro::reader(file)?;

            for value in reader {
                let user = apache_avro::from_value::<User>(&value?)?;

                println!("{},{}", user.id(), user.snapshot);
            }
        }
    }

    Ok(())
}

fn write_from_path<P: AsRef<Path>, W: Write>(
    path: P,
    writer: &mut apache_avro::Writer<W>,
) -> Result<(), Error> {
    eprintln!("Reading file: {:?}", path.as_ref().to_string_lossy());
    let lines = lines(path)?;

    for (i, line) in lines.enumerate() {
        let user = match serde_json::from_str::<User>(&line?) {
            Ok(value) => value,
            Err(error) => {
                panic!("At {}: {:?}", i, error);
            }
        };
        writer.append_ser(user)?;
    }

    Ok(())
}

fn lines<P: AsRef<Path>>(
    path: P,
) -> Result<Box<dyn Iterator<Item = Result<String, std::io::Error>>>, std::io::Error> {
    let extension = path
        .as_ref()
        .extension()
        .and_then(|extension| extension.to_str())
        .map(|extension| extension.to_lowercase());
    let file = File::open(path)?;

    if extension == Some("gz".to_string()) {
        Ok(Box::new(BufReader::new(GzDecoder::new(file)).lines()))
    } else {
        Ok(Box::new(BufReader::new(file).lines()))
    }
}

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("I/O error")]
    Io(#[from] std::io::Error),
    #[error("Avro error")]
    Avro(#[from] apache_avro::Error),
    #[error("User profile Avro error")]
    UserAvro(#[from] twprs::avro::Error),
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
    Dump {
        /// Input path
        #[clap(short, long)]
        input: String,
    },
}
