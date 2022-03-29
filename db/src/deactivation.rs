use chrono::{DateTime, TimeZone, Utc};
use egg_mode_extras::client::FormerUserStatus;
use std::collections::{HashMap, HashSet};
use std::convert::TryFrom;
use std::io::{BufRead, BufReader, BufWriter, Read, Write};
use std::ops::Add;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("I/O error")]
    Io(#[from] std::io::Error),
    #[error("Invalid user ID")]
    InvalidUserId(Option<String>),
    #[error("Invalid timestamp")]
    InvalidTimestamp(Option<String>),
    #[error("Invalid Twitter error code")]
    InvalidTwitterErrorCode(Option<String>),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Entry {
    pub status: FormerUserStatus,
    pub observed: DateTime<Utc>,
    pub reversal: Option<DateTime<Utc>>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Log {
    entries: HashMap<u64, Vec<Entry>>,
}

impl Log {
    pub fn status(&self, user_id: u64) -> Option<FormerUserStatus> {
        self.entries.get(&user_id).and_then(|entries| {
            entries.iter().find_map(|entry| {
                if entry.reversal.is_none() {
                    Some(entry.status)
                } else {
                    None
                }
            })
        })
    }

    pub fn current_deactivated(&self) -> HashSet<u64> {
        self.entries
            .iter()
            .filter_map(|(user_id, entries)| {
                if entries.iter().all(|entry| entry.reversal.is_none()) {
                    Some(*user_id)
                } else {
                    None
                }
            })
            .collect()
    }

    pub fn ever_deactivated(&self) -> HashSet<u64> {
        self.entries.keys().cloned().collect()
    }

    pub fn current_suspended(&self) -> HashSet<u64> {
        self.entries
            .iter()
            .filter_map(|(user_id, entries)| {
                if entries.iter().all(|entry| entry.reversal.is_some())
                    && entries.last().map(|entry| entry.status) == Some(FormerUserStatus::Suspended)
                {
                    Some(*user_id)
                } else {
                    None
                }
            })
            .collect()
    }

    pub fn ever_suspended(&self) -> HashSet<u64> {
        self.entries
            .iter()
            .filter_map(|(user_id, entries)| {
                if entries
                    .iter()
                    .any(|entry| entry.status == FormerUserStatus::Suspended)
                {
                    Some(*user_id)
                } else {
                    None
                }
            })
            .collect()
    }

    pub fn update<I: Iterator<Item = (u64, DateTime<Utc>)>>(
        &mut self,
        reversals: I,
    ) -> Result<(), Vec<(u64, DateTime<Utc>)>> {
        let mut invalid_pairs = vec![];

        for (user_id, timestamp) in reversals {
            match self
                .entries
                .get_mut(&user_id)
                .and_then(|entries| entries.last_mut())
            {
                Some(last) => {
                    if last.reversal.is_none() {
                        last.reversal = Some(timestamp);
                    } else {
                        invalid_pairs.push((user_id, timestamp));
                    }
                }
                None => {
                    invalid_pairs.push((user_id, timestamp));
                }
            }
        }

        if invalid_pairs.is_empty() {
            Ok(())
        } else {
            Err(invalid_pairs)
        }
    }

    pub fn validate(&self) -> Result<(), Vec<u64>> {
        let invalid_user_ids = self
            .entries
            .iter()
            .filter_map(|(user_id, entries)| {
                if !entries.is_empty() && Self::validate_entries(&entries) {
                    None
                } else {
                    Some(*user_id)
                }
            })
            .collect::<Vec<_>>();

        if invalid_user_ids.is_empty() {
            Ok(())
        } else {
            Err(invalid_user_ids)
        }
    }

    fn validate_entries(entries: &[Entry]) -> bool {
        let valid_pairs = entries.windows(2).all(|pair| match pair[0].reversal {
            Some(reversal) => pair[0].observed < reversal && pair[0].observed < pair[1].observed,
            None => false,
        });

        // We still have to checked whether the reversal (if there was one) for the final entry
        // happened after the observation.
        valid_pairs
            && match entries.last() {
                Some(entry) => match entry.reversal {
                    Some(reversal) => entry.observed < reversal,
                    None => true,
                },
                None => true,
            }
    }

    pub fn read<R: Read>(reader: R) -> Result<Self, Error> {
        let mut entries: HashMap<u64, Vec<Entry>> = HashMap::new();

        for line in BufReader::new(reader).lines() {
            let line = line?;
            let fields = line.split(',').collect::<Vec<_>>();

            let user_id = fields
                .get(0)
                .and_then(|value| value.parse::<u64>().ok())
                .ok_or_else(|| {
                    Error::InvalidUserId(fields.get(0).map(|value| value.to_string()))
                })?;

            let status = fields
                .get(1)
                .and_then(|value| value.parse::<i32>().ok())
                .and_then(|value| FormerUserStatus::try_from(value).ok())
                .ok_or_else(|| {
                    Error::InvalidTwitterErrorCode(fields.get(1).map(|value| value.to_string()))
                })?;

            let observed = fields
                .get(2)
                .and_then(|value| value.parse::<i64>().ok())
                .map(|value| Utc.timestamp(value, 0))
                .ok_or_else(|| {
                    Error::InvalidTimestamp(fields.get(2).map(|value| value.to_string()))
                })?;

            let reversal = fields
                .get(3)
                .and_then(|value| {
                    if value.is_empty() {
                        Some(None)
                    } else {
                        value
                            .parse::<i64>()
                            .ok()
                            .map(|value| Some(Utc.timestamp(value, 0)))
                    }
                })
                .ok_or_else(|| {
                    Error::InvalidTimestamp(fields.get(3).map(|value| value.to_string()))
                })?;

            let seen = entries.entry(user_id).or_default();
            seen.push(Entry {
                status,
                observed,
                reversal,
            });
        }

        Ok(Self { entries })
    }

    pub fn deactivations(
        &self,
    ) -> Vec<(u64, FormerUserStatus, DateTime<Utc>, Option<DateTime<Utc>>)> {
        let mut entries = self.entries.iter().collect::<Vec<_>>();
        entries.sort_by_key(|(user_id, _)| *user_id);

        entries
            .iter()
            .flat_map(|(user_id, entries)| {
                entries
                    .iter()
                    .map(|entry| (**user_id, entry.status, entry.observed, entry.reversal))
            })
            .collect()
    }

    pub fn suspensions(&self) -> Vec<(u64, DateTime<Utc>, Option<DateTime<Utc>>)> {
        let mut entries = self.entries.iter().collect::<Vec<_>>();
        entries.sort_by_key(|(user_id, _)| *user_id);

        entries
            .iter()
            .flat_map(|(user_id, entries)| {
                entries.iter().filter_map(|entry| {
                    if entry.status == FormerUserStatus::Suspended {
                        Some((**user_id, entry.observed, entry.reversal))
                    } else {
                        None
                    }
                })
            })
            .collect()
    }

    pub fn write<W: Write>(&self, writer: W) -> Result<(), std::io::Error> {
        let mut entries = self.entries.iter().collect::<Vec<_>>();
        entries.sort_by_key(|(user_id, _)| *user_id);

        let mut writer = BufWriter::new(writer);

        for (user_id, entries) in entries {
            for entry in entries {
                writeln!(
                    writer,
                    "{},{},{},{}",
                    user_id,
                    entry.status.code(),
                    entry.observed.timestamp(),
                    entry
                        .reversal
                        .map(|value| value.timestamp().to_string())
                        .unwrap_or_default()
                )?;
            }
        }

        Ok(())
    }
}

impl Add for &Log {
    type Output = Log;

    fn add(self, other: Self) -> Self::Output {
        let mut new_entry_map = self.entries.clone();

        for (user_id, entries) in &other.entries {
            let new_entries = new_entry_map.entry(*user_id).or_default();
            new_entries.extend(entries.clone());
            new_entries.sort_by_key(|entry| entry.observed);
            new_entries.dedup();

            let len = new_entries.len();
            if len >= 2 {
                let last1 = &new_entries[len - 2];
                let last2 = &new_entries[len - 1];
                if last1.status == last2.status
                    && last1.reversal.is_none()
                    && last2.reversal.is_none()
                {
                    new_entries.pop();
                }
            }
        }

        Self::Output {
            entries: new_entry_map,
        }
    }
}
