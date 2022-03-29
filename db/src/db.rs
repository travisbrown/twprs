use apache_avro::{from_avro_datum, from_value, to_avro_datum, to_value};
use rocksdb::{DBCompressionType, Options, DB};
use std::io::Cursor;
use std::path::Path;
use std::sync::Arc;
use twprs::{avro::USER_SCHEMA, model::User};

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("I/O error")]
    Io(#[from] std::io::Error),
    #[error("UTF-8 decoding error")]
    Utf8(#[from] std::str::Utf8Error),
    #[error("RocksDb error")]
    Db(#[from] rocksdb::Error),
    #[error("Avro decoding error")]
    Avro(#[from] apache_avro::Error),
    #[error("Invalid key")]
    InvalidKey(Vec<u8>),
}

#[derive(Clone)]
pub struct ProfileDb {
    db: Arc<DB>,
    options: Options,
}

impl ProfileDb {
    pub fn open<P: AsRef<Path>>(path: P, enable_statistics: bool) -> Result<Self, Error> {
        let mut options = Options::default();
        options.create_if_missing(true);
        options.set_compression_type(DBCompressionType::Zstd);

        if enable_statistics {
            options.enable_statistics();
        }

        let db = DB::open(&options, path)?;

        Ok(Self {
            db: Arc::new(db),
            options,
        })
    }

    pub fn statistics(&self) -> Option<String> {
        self.options.get_statistics()
    }

    pub fn lookup(&self, user_id: u64) -> Result<Vec<User>, Error> {
        let prefix = user_id.to_be_bytes();
        let iterator = self.db.prefix_iterator(prefix);
        let mut users: Vec<User> = vec![];

        for (key, value) in iterator {
            let next_user_id = u64::from_be_bytes(
                key[0..8]
                    .try_into()
                    .map_err(|_| Error::InvalidKey(key.to_vec()))?,
            );

            if next_user_id == user_id {

            let mut cursor = Cursor::new(value);
            let avro_value = from_avro_datum(&USER_SCHEMA, &mut cursor, None)?;
            let user = from_value(&avro_value)?;
            users.push(user);
            } else {
                break;
            }
        }

        users.sort_by_key(|user| user.snapshot);

        Ok(users)
    }

    pub fn update(&self, user: &User) -> Result<(), Error> {
        let key = Self::make_key(user.id, &user.screen_name);
        let avro_value = to_value(user)?;
        let value = to_avro_datum(&USER_SCHEMA, avro_value)?;
        Ok(self.db.put(key, value)?)
    }

    fn make_key(user_id: i64, screen_name: &str) -> Vec<u8> {
        let screen_name_clean = screen_name.to_lowercase();
        let screen_name_bytes = screen_name_clean.as_bytes();
        let mut key = Vec::with_capacity(screen_name_bytes.len() + 8);
        key.extend_from_slice(&user_id.to_be_bytes());
        key.extend_from_slice(&screen_name_bytes);
        key
    }
}
