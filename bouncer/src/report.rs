use chrono::{DateTime, Duration, TimeZone, Utc};
use egg_mode::user::TwitterUser;
use egg_mode_extras::{client::TokenType, Client};
use futures::TryStreamExt;
use serde_json::Value;
use std::collections::{HashMap, HashSet};
use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use twprs::model::User;
use twprs_db::db::ProfileDb;

pub struct Report {
    client: Arc<Client>,
    base: PathBuf,
    user: TwitterUser,
    follower_ids: HashSet<u64>,
    followed_ids: HashSet<u64>,
    missing_user_ids: HashSet<u64>,
    known_users: HashMap<u64, Vec<User>>,
}

impl Report {
    pub async fn new<P: AsRef<Path>>(
        client: Arc<Client>,
        base: P,
        screen_name: String,
    ) -> Result<Self, Error> {
        // We use a user token here in part to make sure the target account is not blocked.
        let user = client
            .lookup_user(screen_name.clone(), TokenType::User)
            .await?;

        let follower_ids = client
            .follower_ids(screen_name.clone(), TokenType::App)
            .try_collect::<HashSet<_>>();
        let followed_ids = client
            .followed_ids(screen_name, TokenType::App)
            .try_collect::<HashSet<_>>();

        let (follower_ids, followed_ids) = futures::try_join!(follower_ids, followed_ids)?;
        let missing_user_ids = follower_ids.union(&followed_ids).cloned().collect();

        Ok(Self {
            client,
            base: base.as_ref().to_path_buf(),
            user,
            follower_ids,
            followed_ids,
            missing_user_ids,
            known_users: HashMap::new(),
        })
    }

    pub async fn load<P: AsRef<Path>>(
        client: Arc<Client>,
        base: P,
        screen_name: String,
    ) -> Result<Self, Error> {
        let user = client
            .lookup_user(screen_name.clone(), TokenType::User)
            .await?;

        let directory = Self::directory_from_base(&base, user.id);

        let follower_ids = read_ids(directory.join("followers.txt"))?;
        let followed_ids = read_ids(directory.join("following.txt"))?;
        let missing_user_ids = follower_ids.union(&followed_ids).cloned().collect();

        Ok(Self {
            client,
            base: base.as_ref().to_path_buf(),
            user,
            follower_ids,
            followed_ids,
            missing_user_ids,
            known_users: HashMap::new(),
        })
    }

    pub fn save(&self, bad: &HashMap<u64, usize>) -> Result<(), Error> {
        let directory = self.directory();
        std::fs::create_dir_all(&directory)?;

        let bad_followers = self.bad_followers(bad);
        let bad_followeds = self.bad_followeds(bad);
        let non_mutual_protected_followers = self.non_mutual_protected_followers();

        write_ids(
            directory.join("followers.txt"),
            self.follower_ids.iter().cloned(),
        )?;
        write_ids(
            directory.join("following.txt"),
            self.followed_ids.iter().cloned(),
        )?;

        let mut readme_writer = File::create(directory.join("README.md"))?;

        writeln!(
            readme_writer,
            "# Follower report for {}",
            self.user.screen_name
        )?;
        writeln!(readme_writer, "This first two tables in this report list Twitter accounts that follow or are followed by")?;
        writeln!(
            readme_writer,
            "[{}](https://twitter.com/{})",
            self.user.screen_name, self.user.screen_name
        )?;
        writeln!(
            readme_writer,
            "that have been identified as related to far-right networks on Twitter."
        )?;
        writeln!(readme_writer)?;
        writeln!(
            readme_writer,
            "Please note that not these lists are automatically generated from follower networks,"
        )?;
        writeln!(
            readme_writer,
            "and the flagged accounts may include journalists, researchers, or others"
        )?;
        writeln!(
            readme_writer,
            "who follow far-right accounts without endorsing far-right ideologies."
        )?;
        writeln!(readme_writer, "This report is intended for manual review, and should not be used for automated blocking.")?;
        writeln!(readme_writer)?;
        writeln!(
            readme_writer,
            "The third table lists protected accounts that follow but are not followed by"
        )?;
        writeln!(
            readme_writer,
            "[{}](https://twitter.com/{}).",
            self.user.screen_name, self.user.screen_name
        )?;
        writeln!(readme_writer, "These accounts may include sock puppets operated by individuals who have been blocked by the user.")?;

        writeln!(readme_writer, "## Table of contents")?;
        writeln!(
            readme_writer,
            "* [Flagged followers](#flagged-followers) ({})",
            bad_followers.len()
        )?;
        writeln!(
            readme_writer,
            "* [Flagged following](#flagged-following) ({})",
            bad_followeds.len()
        )?;
        writeln!(
            readme_writer,
            "* [Possible socks](#possible-socks) ({})",
            non_mutual_protected_followers.len()
        )?;
        writeln!(readme_writer)?;

        writeln!(readme_writer, "## Flagged followers")?;
        writeln!(
            readme_writer,
            "{} followers are flagged as needing review. 🔄 indicates that the account is a mutual, ✔️ that it is verified, and 🔒 that it is locked.",
            bad_followers.len()
        )?;

        if !bad_followers.is_empty() {
            writeln!(readme_writer, "<table>")?;
            writeln!(readme_writer, "<tr><th></th><th align=\"left\">Ranking</th><th align=\"left\">Twitter ID</th><th align=\"left\">Screen name</th>")?;
            writeln!(
                readme_writer,
                "<th align=\"left\">Created</th><th align=\"left\">Status</th>"
            )?;
            writeln!(readme_writer, "<th align=\"left\">Followers</th></tr>")?;

            for (ranking, user_id, profiles) in bad_followers {
                if let Some(last) = profiles.last() {
                    let other_screen_names = profiles
                        .iter()
                        .take(profiles.len() - 1)
                        .map(|profile| profile.screen_name.clone())
                        .collect::<Vec<_>>();
                    let row = user_row(
                        last,
                        self.followed_ids.contains(&user_id),
                        other_screen_names.as_slice(),
                        Some(ranking),
                    )?;
                    writeln!(readme_writer, "{}", row)?;
                }
            }
            writeln!(readme_writer, "</table>\n")?;
        }

        writeln!(readme_writer, "## Flagged following")?;
        writeln!(
            readme_writer,
            "{} followers are flagged as needing review. 🔄 indicates that the account is a mutual, ✔️ that it is verified, and 🔒 that it is locked.",
            bad_followeds.len()
        )?;

        if !bad_followeds.is_empty() {
            writeln!(readme_writer, "<table>")?;
            writeln!(readme_writer, "<tr><th></th><th align=\"left\">Ranking</th><th align=\"left\">Twitter ID</th><th align=\"left\">Screen name</th>")?;
            writeln!(
                readme_writer,
                "<th align=\"left\">Created</th><th align=\"left\">Status</th>"
            )?;
            writeln!(readme_writer, "<th align=\"left\">Followers</th></tr>")?;

            for (ranking, user_id, profiles) in bad_followeds {
                if let Some(last) = profiles.last() {
                    let other_screen_names = profiles
                        .iter()
                        .take(profiles.len() - 1)
                        .map(|profile| profile.screen_name.clone())
                        .collect::<Vec<_>>();
                    let row = user_row(
                        last,
                        self.follower_ids.contains(&user_id),
                        other_screen_names.as_slice(),
                        Some(ranking),
                    )?;
                    writeln!(readme_writer, "{}", row)?;
                }
            }
            writeln!(readme_writer, "</table>\n")?;
        }

        writeln!(readme_writer, "## Possible socks")?;
        writeln!(
            readme_writer,
            "{} non-mutual protected accounts are flagged as needing review. ✔️ indicates that the account is verified and 🔒 that it is locked.",
            non_mutual_protected_followers.len()
        )?;
        writeln!(readme_writer, "<table>")?;
        writeln!(
            readme_writer,
            "<tr><th><th align=\"left\">Ranking</th></th><th align=\"left\">Twitter ID</th><th align=\"left\">Screen name</th>"
        )?;
        writeln!(
            readme_writer,
            "<th align=\"left\">Created</th><th align=\"left\">Status</th>"
        )?;
        writeln!(readme_writer, "<th align=\"left\">Followers</th></tr>")?;

        for (user_id, profiles) in non_mutual_protected_followers {
            if let Some(last) = profiles.last() {
                let other_screen_names = profiles
                    .iter()
                    .take(profiles.len() - 1)
                    .map(|profile| profile.screen_name.clone())
                    .collect::<Vec<_>>();
                let row = user_row(
                    last,
                    false,
                    other_screen_names.as_slice(),
                    bad.get(&user_id).cloned(),
                )?;
                writeln!(readme_writer, "{}", row)?;
            }
        }
        writeln!(readme_writer, "</table>\n")?;

        Ok(())
    }

    pub fn bad_followers(&self, bad: &HashMap<u64, usize>) -> Vec<(usize, u64, &[User])> {
        let mut result = vec![];

        for user_id in &self.follower_ids {
            if let Some(ranking) = bad.get(&user_id) {
                if let Some(profiles) = self.known_users.get(&user_id) {
                    result.push((*ranking, *user_id, profiles.as_slice()));
                }
            }
        }

        result.sort_by_key(|(ranking, user_id, _)| (*ranking, *user_id));
        result
    }

    pub fn bad_followeds(&self, bad: &HashMap<u64, usize>) -> Vec<(usize, u64, &[User])> {
        let mut result = vec![];

        for user_id in &self.followed_ids {
            if let Some(ranking) = bad.get(&user_id) {
                if let Some(profiles) = self.known_users.get(&user_id) {
                    result.push((*ranking, *user_id, profiles.as_slice()));
                }
            }
        }

        result.sort_by_key(|(ranking, user_id, _)| (*ranking, *user_id));
        result
    }

    pub fn non_mutual_protected_followers(&self) -> Vec<(u64, &[User])> {
        let mut users = vec![];

        for user_id in self.non_mutual_follower_ids() {
            if let Some(profiles) = self.known_users.get(&user_id) {
                if let Some(last) = profiles.last() {
                    if last.protected {
                        users.push((user_id, profiles.as_slice()));
                    }
                }
            }
        }

        users.sort_by_key(|(user_id, profiles)| {
            (profiles.last().map(|last| last.followers_count), *user_id)
        });
        users
    }

    pub fn total_user_count(&self) -> usize {
        self.all_user_ids().count()
    }

    pub fn missing_user_ids(&self) -> &HashSet<u64> {
        &self.missing_user_ids
    }

    pub fn missing_user_count(&self) -> usize {
        self.missing_user_ids.len()
    }

    pub fn non_mutual_follower_ids(&self) -> HashSet<u64> {
        self.follower_ids
            .difference(&self.followed_ids)
            .cloned()
            .collect()
    }

    fn directory_from_base<P: AsRef<Path>>(base: P, user_id: u64) -> PathBuf {
        base.as_ref().join(format!("{:0>20}", user_id))
    }

    pub fn directory(&self) -> PathBuf {
        Self::directory_from_base(&self.base, self.user.id)
    }

    fn all_user_ids(&self) -> impl Iterator<Item = u64> + '_ {
        self.follower_ids.union(&self.followed_ids).cloned()
    }

    pub fn read_users(
        &mut self,
        db: &ProfileDb,
        max_age: Option<Duration>,
    ) -> Result<usize, Error> {
        let now = Utc::now();

        let directory = self.directory();
        std::fs::create_dir_all(&directory)?;

        let mut local_users = read_local_users(&directory)?;

        for user_id in &self.missing_user_ids {
            let mut profiles = db
                .lookup(*user_id)?
                .into_iter()
                .map(|(_, profile)| profile)
                .collect::<Vec<_>>();

            profiles.extend(local_users.remove(user_id).unwrap_or_default());
            profiles.sort_by_key(|profile| profile.snapshot);

            if let Some(last) = profiles.last() {
                if max_age
                    .filter(|limit| now - Utc.timestamp(last.snapshot, 0) > *limit)
                    .is_none()
                {
                    self.known_users.insert(last.id(), profiles);
                }
            }
        }

        self.missing_user_ids
            .retain(|user_id| !self.known_users.contains_key(user_id));

        Ok(self.known_users.len())
    }

    pub async fn download_missing_users(&mut self) -> Result<usize, Error> {
        let mut missing_user_ids = self.missing_user_ids.iter().cloned().collect::<Vec<_>>();
        missing_user_ids.sort_unstable();

        let mut missing_users = self
            .client
            .lookup_users_json(missing_user_ids, TokenType::App)
            .map_err(Error::from)
            .and_then(|mut value| async {
                let user_id = extract_user_id(&value)?;
                let timestamp = timestamp_json(&mut value)?;

                Ok((user_id, timestamp, value))
            })
            .try_collect::<Vec<_>>()
            .await?;

        let count = missing_users.len();

        missing_users.sort_by_key(|(user_id, timestamp, _)| (*timestamp, *user_id));

        let directory = self.directory();
        std::fs::create_dir_all(&directory)?;

        if !missing_users.is_empty() {
            let now = Utc::now();
            let file = OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(directory.join(format!("{}.ndjson.gz", now.timestamp())))?;
            let mut writer = flate2::write::GzEncoder::new(file, flate2::Compression::default());

            for (user_id, _, profile_value) in missing_users {
                writeln!(writer, "{}", profile_value)?;

                self.known_users.insert(
                    user_id,
                    vec![serde_json::from_value::<User>(profile_value)?],
                );
            }

            self.missing_user_ids
                .retain(|user_id| !self.known_users.contains_key(user_id));

            writer.try_finish()?;
        }

        Ok(count)
    }
}

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("egg-mode error")]
    EggMode(#[from] egg_mode::error::Error),
    #[error("egg-mode-extras error")]
    EggModeExtras(#[from] egg_mode_extras::error::Error),
    #[error("ProfileDb error")]
    ProfileDb(#[from] twprs_db::db::Error),
    #[error("JSON encoding error")]
    Json(#[from] serde_json::Error),
    #[error("I/O error")]
    Io(#[from] std::io::Error),
    #[error("Deactivations file parsing error")]
    DeactivationsFile(#[from] twprs_db::deactivation::Error),
    #[error("Unexpected user JSON object")]
    UnexpectedUserJsonObject(Value),
    #[error("Unexpected CSV line")]
    UnexpectedCsvLine(String),
    #[error("Unexpected ID line")]
    UnexpectedIdLine(String),
    #[error("Date format error")]
    DateFormat(#[from] chrono::ParseError),
}

fn timestamp_json(value: &mut Value) -> Result<DateTime<Utc>, Error> {
    if let Some(fields) = value.as_object_mut() {
        let timestamp = Utc::now();

        if let Some(_previous_value) = fields.insert(
            "snapshot".to_string(),
            serde_json::json!(timestamp.timestamp()),
        ) {
            Err(Error::UnexpectedUserJsonObject(value.clone()))
        } else {
            Ok(timestamp)
        }
    } else {
        Err(Error::UnexpectedUserJsonObject(value.clone()))
    }
}

fn extract_user_id(value: &Value) -> Result<u64, Error> {
    value
        .get("id_str")
        .and_then(|id_str_value| id_str_value.as_str())
        .and_then(|id_str| id_str.parse::<u64>().ok())
        .ok_or_else(|| Error::UnexpectedUserJsonObject(value.clone()))
}

fn read_ids<P: AsRef<Path>>(path: P) -> Result<HashSet<u64>, Error> {
    let reader = BufReader::new(File::open(path)?);

    reader
        .lines()
        .map(|line| {
            let line = line?;
            let id = line
                .parse::<u64>()
                .map_err(|_| Error::UnexpectedIdLine(line.clone()))?;

            Ok(id)
        })
        .collect()
}

fn write_ids<P: AsRef<Path>, I: Iterator<Item = u64>>(path: P, ids: I) -> Result<(), Error> {
    let mut writer = File::create(path)?;

    for id in ids {
        writeln!(writer, "{}", id)?;
    }

    Ok(())
}

fn read_local_users<P: AsRef<Path>>(directory: P) -> Result<HashMap<u64, Vec<User>>, Error> {
    let mut result: HashMap<u64, Vec<User>> = HashMap::new();

    for entry in std::fs::read_dir(directory)? {
        let entry = entry?;
        let path = entry.path();

        if path
            .file_name()
            .and_then(|os_str| os_str.to_str())
            .filter(|as_str| as_str.ends_with(".ndjson.gz"))
            .is_some()
        {
            let reader = BufReader::new(flate2::read::GzDecoder::new(File::open(path)?));

            for line in reader.lines() {
                let line = line?;
                let user = serde_json::from_str::<User>(&line)?;

                let profiles = result.entry(user.id()).or_default();
                profiles.push(user);
            }
        }
    }

    Ok(result)
}

fn user_row(
    user: &User,
    mutual: bool,
    screen_names: &[String],
    ranking: Option<usize>,
) -> Result<String, Error> {
    let img = format!(
        "<a href=\"{}\"><img src=\"{}\" width=\"40px\" height=\"40px\" align=\"center\"/></a>",
        user.profile_image_url_https, user.profile_image_url_https
    );
    let id_link = format!(
        "<a href=\"https://twitter.com/intent/user?user_id={}\">{}</a>",
        user.id, user.id
    );

    let screen_name_link = format!(
        "<a href=\"https://twitter.com/{}\">{}</a>{}",
        user.screen_name,
        user.screen_name,
        if screen_names.is_empty() {
            "".to_string()
        } else {
            format!(
                ", {}",
                screen_names
                    .into_iter()
                    .take(1)
                    .cloned()
                    .collect::<Vec<_>>()
                    .join(", ")
            )
        }
    );

    let created_at = user.created_at()?.format("%Y-%m-%d");

    let mut statuses = Vec::new();
    if mutual {
        statuses.push("🔄");
    }
    if user.protected {
        statuses.push("🔒");
    }
    if user.verified {
        statuses.push("✔️");
    }

    Ok(format!(
        "<tr><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td align=\"center\">{}</td><td>{}</td></tr>",
        img,
        ranking.map(|value| value.to_string()).unwrap_or_default(),
        id_link,
        screen_name_link,
        created_at,
        statuses.join(" "),
        user.followers_count,
    ))
}
