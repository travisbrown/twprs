use clap::Parser;
use std::cmp::Reverse;
use std::collections::HashMap;
use std::fs::File;
use std::io::Write;
use twprs::model::User;
use twprs_db::db::ProfileDb;

fn main() -> Result<(), Error> {
    let opts: Opts = Opts::parse();
    twprs::cli::init_logging(opts.verbose)?;
    let db = ProfileDb::open(opts.db, true)?;

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
        Command::Count => {
            let mut user_count = 0;
            let mut screen_name_count = 0;
            let mut verified = 0;
            let mut protected = 0;
            for result in db.iter() {
                let batch = result?;

                user_count += 1;
                screen_name_count += batch.len();

                if let Some((_, profile)) = batch.last() {
                    if profile.verified {
                        verified += 1;
                    }
                    if profile.protected {
                        protected += 1;
                    }
                }
            }

            println!("{} users, {} screen names", user_count, screen_name_count);
            println!("{} verified, {} protected", verified, protected);
        }
        Command::CountRaw => {
            let mut user_ids = std::collections::HashSet::new();
            let mut screen_name_count = 0;

            for result in db.raw_iter() {
                let (user_id, (_, user)) = result?;

                user_ids.insert(user_id);
                screen_name_count += 1;
            }

            println!(
                "{} users, {} screen names",
                user_ids.len(),
                screen_name_count
            );
        }
        Command::Between { first, last } => {
            let mut profiles = vec![];

            for result in db.raw_iter() {
                let (_, (_, user)) = result?;

                if user.snapshot > first && user.snapshot < last {
                    profiles.push(user);
                }
            }

            profiles.sort_by_key(|profile| (profile.snapshot, profile.id));

            for profile in profiles {
                println!("{}", serde_json::to_value(profile)?);
            }
        }
        Command::Stats => {
            println!("Estimate the number of keys: {}", db.estimate_key_count()?);
            println!("{:?}", db.statistics());
        }
        Command::ScreenNames => {
            for result in db.iter() {
                let batch = result?;
                if let Some((_, most_recent)) = batch.last() {
                    println!("{},{}", most_recent.id, most_recent.screen_name);
                } else {
                    log::error!("Empty user result when reading database");
                }
            }
        }
        Command::SnapshotAge { count } => {
            let mut queue = priority_queue::DoublePriorityQueue::with_capacity(count);
            for result in db.iter() {
                let batch = result?;
                if let Some((_, most_recent)) = batch.last() {
                    let snapshot = Reverse(most_recent.snapshot);
                    let min = queue.peek_min().map(|(_, snapshot)| *snapshot);

                    if min.filter(|&value| snapshot >= value).is_some() || queue.len() < count {
                        queue.push(most_recent.id, snapshot);

                        if queue.len() > count {
                            queue.pop_min();
                        }
                    }
                } else {
                    log::error!("Empty user result when reading database");
                }
            }

            for (id, snapshot) in queue.into_sorted_iter().rev() {
                println!("{},{}", id, snapshot.0);
            }
        }
        Command::AllScreenNames => {
            for result in db.iter() {
                let batch = result?;

                for (first, profile) in batch {
                    println!(
                        "{},{},{},{}",
                        profile.id,
                        profile.screen_name,
                        first.timestamp(),
                        profile.snapshot
                    );
                }
            }
        }
        Command::Statuses => {
            for result in db.iter() {
                let batch = result?;
                if let Some((_, most_recent)) = batch.last() {
                    println!(
                        "{},{},{},{},{}",
                        most_recent.id,
                        most_recent.screen_name,
                        most_recent.followers_count,
                        most_recent.friends_count,
                        if most_recent.protected { 1 } else { 0 },
                    );
                } else {
                    log::error!("Empty user result when reading database");
                }
            }
        }
        Command::Withheld => {
            for result in db.iter() {
                let batch = result?;

                if batch
                    .iter()
                    .any(|(_, user)| !user.withheld_in_countries.is_empty())
                {
                    if let Some((_, most_recent)) = batch.last() {
                        println!("{}", serde_json::to_value(most_recent)?);
                    }
                }
            }
        }
        Command::Urls { query } => {
            /*let keywords = query
            .split(",")
            .map(|keyword| keyword.to_lowercase())
            .collect::<Vec<_>>();*/
            let keyword = query.to_lowercase();

            for result in db.iter() {
                let batch = result?;

                for (_, profile) in batch {
                    if let Some(ref entities) = profile.entities {
                        if entities
                            .url
                            .as_ref()
                            .map(|urls| {
                                urls.urls.iter().any(|url| {
                                    url.expanded_url
                                        .as_ref()
                                        .map(|url| url.to_lowercase().contains(&keyword))
                                        .unwrap_or(false)
                                })
                            })
                            .unwrap_or(false)
                            || entities
                                .description
                                .as_ref()
                                .map(|urls| {
                                    urls.urls.iter().any(|url| {
                                        url.expanded_url
                                            .as_ref()
                                            .map(|url| url.to_lowercase().contains(&keyword))
                                            .unwrap_or(false)
                                    })
                                })
                                .unwrap_or(false)
                        {
                            println!("{}", serde_json::to_value(profile)?);
                        }
                    }
                }
            }

            /*let hits = keywords
                    .iter()
                    .map(|keyword| {
                        batch.iter().any(|(_, profile)| {
                            if let Some(entities) = profile.entities {
                                entities
                                    .url
                                    .map(|urls| {
                                        urls.exists(|url| {
                                            url.expanded_url
                                                .map(|url| url.to_lowercase().contains(keyword))
                                                .unwrap_or(false)
                                        })
                                    })
                                    .unwrap_or(false)
                                    || entities
                                        .description
                                        .map(|urls| {
                                            urls.exists(|url| {
                                                url.expanded_url
                                                    .map(|url| url.to_lowercase().contains(keyword))
                                                    .unwrap_or(false)
                                            })
                                        })
                                        .unwrap_or(false)
                            } else {
                                false
                            }
                        })
                    })
                    .collect::<Vec<_>>();

                if hits.iter().any(|hit| *hit) {
                    if let Some((_, most_recent)) = batch.last() {
                        let results = hits
                            .iter()
                            .map(|hit| if *hit { "1" } else { "0" })
                            .collect::<Vec<_>>()
                            .join(",");
                        println!(
                            "{},{},{},{},{}",
                            most_recent.id,
                            most_recent.screen_name,
                            most_recent.followers_count,
                            most_recent.friends_count,
                            results
                        );
                    }
                }
            }*/
        }
        Command::Bio { query } => {
            let keywords = query
                .split(',')
                .map(|keyword| keyword.to_lowercase())
                .collect::<Vec<_>>();

            for result in db.iter() {
                let batch = result?;

                let hits = keywords
                    .iter()
                    .map(|keyword| {
                        batch.iter().any(|(_, profile)| {
                            profile
                                .description
                                .as_ref()
                                .map(|description| description.to_lowercase().contains(keyword))
                                .unwrap_or(false)
                        })
                    })
                    .collect::<Vec<_>>();

                if hits.iter().any(|hit| *hit) {
                    if let Some((_, most_recent)) = batch.last() {
                        let results = hits
                            .iter()
                            .map(|hit| if *hit { "1" } else { "0" })
                            .collect::<Vec<_>>()
                            .join(",");
                        println!(
                            "{},{},{},{},{}",
                            most_recent.id,
                            most_recent.screen_name,
                            most_recent.followers_count,
                            most_recent.friends_count,
                            results
                        );
                    }
                }
            }

            /*let mut matches = vec![];
            for result in db.iter() {
                let batch = result?;

                let hits = keywords
                    .iter()
                    .map(|keyword| {
                        //batch.iter().any(|(_, profile)| {
                        batch
                            .last()
                            .map(|(_, profile)| {
                                profile
                                    .description
                                    .as_ref()
                                    .map(|description| description.to_lowercase().contains(keyword))
                                    .unwrap_or(false)
                            })
                            .unwrap_or(false)
                    })
                    .collect::<Vec<_>>();

                //if hits.iter().any(|hit| *hit) {
                //
                if hits.iter().all(|hit| *hit) {
                    if let Some((_, most_recent)) = batch.last() {
                        /*let results = hits
                            .iter()
                            .map(|hit| if *hit { "1" } else { "0" })
                            .collect::<Vec<_>>()
                            .join(",");
                        println!(
                            "{},{},{},{},{}",
                            most_recent.id,
                            most_recent.screen_name,
                            most_recent.followers_count,
                            most_recent.friends_count,
                            results
                        );*/
                        let profile = most_recent;
                        //print!("<tr><td><a href=\"{}\"><img src=\"{}\" width=\"40px\" height=\"40px\" align=\"center\"/></a></td>", profile.profile_image_url_https, profile.profile_image_url_https);
                        let a = format!("<td><a href=\"https://twitter.com/intent/user?user_id={}\">{}</a></td>", profile.id, profile.id);
                        let b = format!(
                            "<td><a href=\"https://twitter.com/{}\">{}</a></td>",
                            profile.screen_name, profile.screen_name
                        );
                        let c = format!(
                            "<td>{}</td><td>{}</td></tr>",
                            profile.followers_count,
                            profile
                                .description
                                .as_ref()
                                .unwrap_or(&"".to_string())
                                .replace("\n", " ")
                        );

                        matches.push((profile.followers_count, format!("{}{}{}", a, b, c)));
                    } else {
                        log::error!("Empty user result when reading database");
                    }
                }
            }

            matches.sort_by_key(|(c, _)| std::cmp::Reverse(*c));

            for (_, m) in matches {
                println!("{}", m);
            }*/
        }
        Command::SuspensionReport { deactivations } => {
            let log = twprs_db::deactivation::Log::read(File::open(deactivations)?)?;
            let suspended_user_ids = log.ever_suspended();

            let mut suspended_user_profiles: HashMap<u64, User> = HashMap::new();
            let mut screen_name_change_user_profiles: HashMap<u64, Vec<_>> = HashMap::new();

            for user_id in &suspended_user_ids {
                let batch = db.lookup(*user_id)?;

                if let Some((_, most_recent)) = batch.last() {
                    if suspended_user_ids.contains(&most_recent.id()) {
                        suspended_user_profiles.insert(most_recent.id(), most_recent.clone());
                    }

                    if batch.len() > 1 {
                        screen_name_change_user_profiles.insert(most_recent.id(), batch);
                    }
                } /* else {
                      log::error!("Empty user result when reading database");
                  }*/
            }

            let mut suspension_report = File::create("suspensions.csv")?;
            let mut not_found = 0;

            for (user_id, suspension, reversal) in log.suspensions() {
                if let Some(profile) = suspended_user_profiles.get(&user_id) {
                    writeln!(
                        suspension_report,
                        "{},{},{},{},{},{},{},{},{},{}",
                        suspension.timestamp(),
                        reversal
                            .map(|timestamp| timestamp.timestamp().to_string())
                            .unwrap_or_default(),
                        profile.id(),
                        profile
                            .created_at()
                            .map(|timestamp| timestamp.timestamp().to_string())
                            .unwrap_or_default(),
                        profile.screen_name,
                        profile.verified,
                        profile.protected,
                        profile.followers_count,
                        profile.profile_image_url_https,
                        profile.withheld_in_countries.join(";")
                    )?;
                } else {
                    writeln!(
                        suspension_report,
                        "{},{},{},,,,,,,",
                        suspension.timestamp(),
                        reversal
                            .map(|timestamp| timestamp.timestamp().to_string())
                            .unwrap_or_default(),
                        user_id
                    )?;
                    not_found += 1;
                }
            }

            log::info!(
                "Could not find profiles for {} suspended accounts",
                not_found
            );
        }
        Command::Reports {
            deactivations,
            suspensions,
            screen_name_changes,
        } => {
            let log = twprs_db::deactivation::Log::read(File::open(deactivations)?)?;
            let suspended_user_ids = log.ever_suspended();

            let mut suspended_user_profiles: HashMap<u64, User> = HashMap::new();
            let mut screen_name_change_user_profiles: HashMap<u64, Vec<_>> = HashMap::new();

            for result in db.iter() {
                let batch = result?;
                if let Some((_, most_recent)) = batch.last() {
                    if suspended_user_ids.contains(&most_recent.id()) {
                        suspended_user_profiles.insert(most_recent.id(), most_recent.clone());
                    }

                    if batch.len() > 1 {
                        screen_name_change_user_profiles.insert(most_recent.id(), batch);
                    }
                } /*else {
                      log::error!("Empty user result when reading database");
                  }*/
            }

            let mut suspension_report = File::create(suspensions)?;
            let mut not_found = 0;

            for (user_id, suspension, reversal) in log.suspensions() {
                if let Some(profile) = suspended_user_profiles.get(&user_id) {
                    writeln!(
                        suspension_report,
                        "{},{},{},{},{},{},{},{},{},{}",
                        suspension.timestamp(),
                        reversal
                            .map(|timestamp| timestamp.timestamp().to_string())
                            .unwrap_or_default(),
                        profile.id(),
                        profile
                            .created_at()
                            .map(|timestamp| timestamp.timestamp().to_string())
                            .unwrap_or_default(),
                        profile.screen_name,
                        profile.verified,
                        profile.protected,
                        profile.followers_count,
                        profile.profile_image_url_https,
                        profile.withheld_in_countries.join(";")
                    )?;
                } else {
                    writeln!(
                        suspension_report,
                        "{},{},{},,,,,,,",
                        suspension.timestamp(),
                        reversal
                            .map(|timestamp| timestamp.timestamp().to_string())
                            .unwrap_or_default(),
                        user_id
                    )?;
                    not_found += 1;
                }
            }

            log::info!(
                "Could not find profiles for {} suspended accounts",
                not_found
            );

            let mut screen_name_change_report = File::create(screen_name_changes)?;

            let mut user_id_vec = screen_name_change_user_profiles
                .into_iter()
                .collect::<Vec<_>>();
            user_id_vec.sort_by_key(|(id, _)| *id);

            for (user_id, profiles) in user_id_vec {
                let mut last_screen_name = "".to_string();

                for (first_timestamp, profile) in profiles {
                    if !last_screen_name.is_empty() {
                        writeln!(
                            screen_name_change_report,
                            "{},{},{},{},{},{},{},{}",
                            first_timestamp.timestamp(),
                            profile.id,
                            profile.verified,
                            profile.protected,
                            profile.followers_count,
                            last_screen_name,
                            profile.screen_name,
                            profile.profile_image_url_https
                        )?;
                    }
                    last_screen_name = profile.screen_name.clone();
                }
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
    #[error("Deactivations file parsing error")]
    DeactivationsFile(#[from] twprs_db::deactivation::Error),
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
    Count,
    CountRaw,
    Between {
        first: i64,
        last: i64,
    },
    Stats,
    ScreenNames,
    AllScreenNames,
    SnapshotAge {
        /// How many oldest values to include
        #[clap(long, default_value = "1000000")]
        count: usize,
    },
    Statuses,
    SuspensionReport {
        /// Deactivations file path
        #[clap(long)]
        deactivations: String,
    },
    Reports {
        /// Deactivations file path
        #[clap(long)]
        deactivations: String,
        /// Suspension report path
        #[clap(long, default_value = "suspensions.csv")]
        suspensions: String,
        /// Screen name change report path
        #[clap(long, default_value = "changed-screen-names.csv")]
        screen_name_changes: String,
    },
    Bio {
        /// Keywords
        #[clap(long)]
        query: String,
    },
    Urls {
        /// Keywords
        #[clap(long)]
        query: String,
    },
    Withheld,
}
