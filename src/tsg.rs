use super::model::User;
use bzip2::read::MultiBzDecoder;
use serde_json::{json, Value};
use std::ffi::OsStr;
use std::fs::File;
use std::io::{BufRead, BufReader, Read, Write};
use std::path::Path;
use tar::Archive;
use zip::ZipArchive;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("I/O error")]
    Io(#[from] std::io::Error),
    #[error("Zip file error")]
    Zip(#[from] zip::result::ZipError),
    #[error("JSON error")]
    Json(#[from] serde_json::error::Error),
    #[error("JSON user extraction error")]
    JsonExtract(#[from] super::extract::Error),
}

fn extract_bz_lines<R: Read>(source: R) -> impl Iterator<Item = Result<Vec<User>, Error>> {
    let reader = BufReader::new(MultiBzDecoder::new(source));

    reader.lines().map(|line| {
        let line = line?;
        let value: Value = serde_json::from_str(&line)?;

        Ok(super::extract::extract_user_objects(&value)?)
    })
}

pub fn extract<P: AsRef<Path>, W: Write>(path: P, mut writer: W) -> Result<(), Error> {
    let zip_extension = OsStr::new("zip");
    let tar_extension = OsStr::new("tar");
    let bz2_extension = OsStr::new("bz2");
    let path = path.as_ref();

    if path.extension() == Some(zip_extension) {
        let file = File::open(path)?;
        let mut archive = ZipArchive::new(file)?;

        let mut file_names = Vec::with_capacity(archive.len());

        for i in 0..archive.len() {
            let file = archive.by_index(i)?;

            if file.name().ends_with("bz2") {
                file_names.push((i, file.name().to_string()));
            }
        }
        file_names.sort_by(|(_, file_name_0), (_, file_name_1)| file_name_0.cmp(file_name_1));

        for (i, _) in file_names {
            let mut file = archive.by_index(i)?;
            let mut batch = vec![];

            for users in extract_bz_lines(&mut file) {
                batch.extend(users?);
            }
            batch.sort_by(|user_0, user_1| {
                (user_0.snapshot, user_0.id).cmp(&(user_1.snapshot, user_1.id))
            });

            for user in batch {
                writeln!(writer, "{}", serde_json::to_string(&json!(user))?)?;
            }
        }
    }

    Ok(())
}
