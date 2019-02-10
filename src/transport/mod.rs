pub mod codec;
pub mod server;
pub mod session;

use super::engine::error;
use super::engine::store::Store;

use std::fs;
use std::path::Path;

pub fn open_db_from<P: AsRef<Path>>(path: P) -> Result<Store, error::Error> {
    match fs::metadata(&path) {
        Err(_) => {
            fs::create_dir_all(&path)?;
            open_db_from(path)
        }
        Ok(_) => {
            let mut key = path.as_ref().to_path_buf();
            let mut value = path.as_ref().to_path_buf();
            let mut buffer = path.as_ref().to_path_buf();
            key.push("toy.k");
            value.push("toy.v");
            buffer.push("toy.b");
            let store = Store::new(&key, &value, &buffer)?;
            Ok(store)
        }
    }
}
