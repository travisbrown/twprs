use super::queue::UserQueue;
use chrono::Utc;
use egg_mode_extras::{client::TokenType, Client};
use futures::TryStreamExt;
use serde_json::Value;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("egg-mode error")]
    EggMode(#[from] egg_mode::error::Error),
    #[error("egg-mode-extras error")]
    EggModeExtras(#[from] egg_mode_extras::error::Error),
    #[error("I/O error")]
    Io(#[from] std::io::Error),
    #[error("Unexpected user JSON object")]
    UnexpectedUserJsonObject(Value),
}
pub struct Scraper {
    client: Client,
    queue: UserQueue,
    batch_size: usize,
}

impl Scraper {
    pub fn new(client: Client, queue: UserQueue, batch_size: usize) -> Self {
        Self {
            client,
            queue,
            batch_size,
        }
    }

    pub async fn run(&self) -> Result<(), Error> {
        loop {
            self.run_batch().await?;
        }
    }

    async fn run_batch(&self) -> Result<(), Error> {
        let next_batch_ids = self.queue.next_batch(self.batch_size).await;

        let results = self
            .client
            .lookup_users_json_or_status(next_batch_ids.iter().copied(), TokenType::User)
            .map_err(Error::from)
            .try_filter_map(|result| async {
                match result {
                    Ok(mut value) => {
                        timestamp_json(&mut value)?;
                        println!("{}", value);
                        let user_id = extract_user_id(&value)?;

                        Ok(Some(Ok(user_id)))
                    }
                    Err((egg_mode::user::UserID::ID(user_id), status)) => {
                        let timestamp = Utc::now().timestamp();
                        let status_code = status.code();
                        println!("{},{},{},", user_id, status_code, timestamp);

                        Ok(Some(Err(user_id)))
                    }
                    Err((_, _)) => Ok(None),
                }
            })
            .try_collect::<Vec<_>>()
            .await?;

        let mut updated_ids = vec![];
        let mut deactivated_ids = vec![];

        for result in results {
            match result {
                Ok(user_id) => {
                    updated_ids.push(user_id);
                }
                Err(user_id) => {
                    deactivated_ids.push(user_id);
                }
            }
        }

        self.queue.process_updates(updated_ids).await;
        self.queue.process_deactivations(deactivated_ids).await;

        Ok(())
    }
}

fn timestamp_json(value: &mut Value) -> Result<(), Error> {
    if let Some(fields) = value.as_object_mut() {
        if let Some(previous_value) = fields.insert(
            "snapshot".to_string(),
            serde_json::json!(Utc::now().timestamp()),
        ) {
            Err(Error::UnexpectedUserJsonObject(value.clone()))
        } else {
            Ok(())
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
