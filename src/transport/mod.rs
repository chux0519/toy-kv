pub mod codec;
pub mod server;
pub mod session;

use super::engine::error;
use super::engine::store::Store;

use std::fs;
use std::path::PathBuf;

pub fn open_db_from(path: &PathBuf) -> Result<Store, error::Error> {
    match fs::metadata(&path) {
        Err(_) => {
            fs::create_dir_all(&path)?;
            open_db_from(path)
        }
        Ok(_) => {
            let mut key = path.clone();
            let mut value = path.clone();
            let mut buffer = path.clone();
            key.push("toy.k");
            value.push("toy.v");
            buffer.push("toy.b");
            let store = Store::new(&key, &value, &buffer)?;
            Ok(store)
        }
    }
}
