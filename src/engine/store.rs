use super::kv::*;
use super::util::*;

use std::fs::File;
use std::io;
use std::path::PathBuf;
use std::sync::RwLock;

use memmap::MmapMut;

/// Seperating keys and values
/// `values` is an insert only vector
/// `index` for ordering the keys in the store
pub struct Store {
    keys: RwLock<MmapMut>,
    buffer: RwLock<MmapMut>,
    values: RwLock<File>,
    index: RwLock<Vec<Key>>,
}

/// For iteraing the store
pub struct StoreIter<'a> {
    store: &'a Store,
    index: usize,
}

impl<'a> StoreIter<'a> {
    pub fn new(store: &'a Store) -> Self {
        StoreIter { store, index: 0 }
    }
}

impl<'a> Iterator for StoreIter<'a> {
    type Item = (InnerKey, InnerValue);

    fn next(&mut self) -> Option<Self::Item> {
        let rindex = self.store.index.read().unwrap();
        let rvalues = self.store.values.read().unwrap();
        // if self.index < rindex.len() {
        //     let key = &rindex[self.index];
        //     let ventry = key.ventry;
        //     self.index += 1;
        //     if let Value::Valid(inner_value) = &rvalues[ventry] {
        //         return Some((key.inner.clone(), inner_value.clone()));
        //     }
        // }
        None
    }
}

const KEY_FILE_SIZE: usize = 65536 * 12;
const BUFFER_SIZE: usize = 4 * 1024; // 4kb buffer szie

impl Store {
    pub fn new(key_file: PathBuf, buffer_file: PathBuf, value_file: PathBuf) -> Self {
        let mmap_key = get_rw_mmap_fd(key_file, KEY_FILE_SIZE);
        let mmap_buffer = get_rw_mmap_fd(buffer_file, BUFFER_SIZE);
        let value_fd = get_rw_fd(&value_file);
        let index = build_index(&mmap_key).unwrap();

        Store {
            keys: RwLock::new(mmap_key),
            buffer: RwLock::new(mmap_buffer),
            values: RwLock::new(value_fd),
            index: RwLock::new(index),
        }
    }

    pub fn get(&self, key: InnerKey) -> Option<InnerValue> {
        let rindex = self.index.read().unwrap();
        let rvalues = self.values.read().unwrap();
        match bsearch(&*rindex, &key) {
            None => return None,
            Some(pos) => {
                let k = &rindex[pos];
                // TODO: Read from file

                // if k.ventry < rvalues.len() {
                //     let v = &rvalues[k.ventry];
                //     match v {
                //         Value::Invalid => return None,
                //         Value::Valid(val) => return Some(val.clone()),
                //     }
                // }
            }
        }
        None
    }

    pub fn put(&mut self, key: InnerKey, value: Value) -> Result<(), io::Error> {
        // let mut windex = self.index.write().unwrap();
        // let mut wvalues = self.values.write().unwrap();
        // let ventry = wvalues.len();
        // wvalues.push(value);
        // // update index
        // let (found, pos) = find_insert_point(&windex, &key);
        // if found {
        //     windex[pos].ventry = ventry;
        // } else {
        //     if pos == windex.len() {
        //         windex.push(Key {
        //             inner: key.clone(),
        //             ventry,
        //         });
        //     } else {
        //         windex.insert(
        //             pos,
        //             Key {
        //                 inner: key.clone(),
        //                 ventry,
        //             },
        //         );
        //     }
        // }
        Ok(())
    }

    pub fn delete(&mut self, key: InnerKey) -> Result<(), io::Error> {
        self.put(key, Value::Invalid)
    }

    pub fn scan(&self) -> StoreIter {
        StoreIter::new(self)
    }
}
