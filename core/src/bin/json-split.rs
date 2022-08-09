use chrono::{Date, TimeZone, Timelike, Utc};
use hst_tw_profiles::model::User;
use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::path::Path;

const DAY_PARTITIONS: u32 = 2;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = std::env::args().collect::<Vec<_>>();
    let path = &args[1];
    let output_dir = &args[2];

    let mut dates_seen: HashMap<Date<Utc>, HashSet<Box<Path>>> = HashMap::new();

    let mut entries = std::fs::read_dir(path)?.collect::<Result<Vec<_>, _>>()?;
    entries.sort_by_key(|entry| entry.path());

    for entry in entries {
        let path = entry.path().into_boxed_path();
        eprintln!("Reading file: {:?}", path);
        let reader = BufReader::new(File::open(&path)?);

        for (i, line) in reader.lines().enumerate() {
            let line = line?;
            let len = line.len();

            match line[len - 11..len - 1].parse::<i64>() {
                Ok(snapshot) => {
                    let snapshot_date = Utc.timestamp(snapshot, 0).date();

                    let date_paths = dates_seen.entry(snapshot_date).or_default();

                    if !date_paths.contains(&path) {
                        date_paths.insert(path.clone());
                    }
                }
                Err(error) => {
                    log::error!("In {:?} at {}: {:?}", path, i + 1, error);
                }
            }
        }
    }

    eprintln!("Done reading dates");

    let mut dates_seen = dates_seen
        .into_iter()
        .map(|(date, paths)| {
            let mut paths = paths.into_iter().collect::<Vec<_>>();
            paths.sort();
            (date, paths)
        })
        .collect::<Vec<_>>();
    dates_seen.sort_by_key(|(date, _)| *date);
    //dates_seen.retain(|(date, _)| date >= &Utc.ymd(2022, 2, 26));

    for (date, paths) in dates_seen {
        eprintln!("Processing date: {}", date);

        let mut output = BufWriter::new(File::create(format!(
            "{}/{}.ndjson",
            output_dir,
            date.format("%Y-%m-%d")
        ))?);

        for partition_id in 0..DAY_PARTITIONS {
            let mut map = HashMap::new();

            for path in &paths {
                let reader = BufReader::new(File::open(path)?);

                for (i, line) in reader.lines().enumerate() {
                    let line = line?;
                    let len = line.len();

                    match line[len - 11..len - 1].parse::<i64>() {
                        Ok(snapshot) => {
                            let snapshot = Utc.timestamp(snapshot, 0);
                            let snapshot_date = snapshot.date();

                            if snapshot_date == date
                                && (snapshot.hour() / (24 / DAY_PARTITIONS)) == partition_id
                            {
                                let user = match serde_json::from_str::<User>(&line) {
                                    Ok(value) => value,
                                    Err(error) => {
                                        panic!(
                                            "At {} ({:?}, {}): {:?}",
                                            i,
                                            path,
                                            &line[0..20],
                                            error
                                        );
                                    }
                                };

                                let key = (user.snapshot, user.id);

                                if let Some(previous_line) = map.get(&key) {
                                    if *previous_line != line {
                                        eprintln!(
                                            "Invalid duplicate: {} at {}",
                                            user.id, user.snapshot
                                        );
                                    }
                                } else {
                                    map.insert(key, line);
                                }
                            }
                        }
                        Err(_) => {}
                    }
                }
            }

            let mut values = map.into_iter().collect::<Vec<_>>();
            values.sort_by_key(|((snapshot, id), _)| (*snapshot, *id));

            for (_, line) in values {
                writeln!(output, "{}", line)?;
            }
        }
    }

    Ok(())
}
