use super::dio::{DirectFile, FileAccess, Mode};
use super::error;
use super::kv::*;
use super::util::{self, *};

use std::io;
use std::path::{Path, PathBuf};
use std::sync::RwLock;

use memmap::MmapMut;

/// Seperating keys and values
/// `values` is an insert only vector
/// `index` for ordering the keys in the store
pub struct Store {
    km: KeyManager,
    vm: ValueManager,
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
        // let rindex = self.store.index.read().unwrap();
        // let rvalues = self.store.values.read().unwrap();
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

impl Store {
    pub fn new(db_file: PathBuf) -> Self {
        let km = KeyManager::new(&db_file);
        let vm = ValueManager::new(&db_file);

        Store { km, vm }
    }

    pub fn get(&self, key: InnerKey) -> Option<InnerValue> {
        let key = self.km.find(&key);
        match key {
            None => return None,
            Some(k) => match self.vm.read(k.ventry).unwrap() {
                Value::Invalid => return None,
                Value::Valid(val) => return Some(val.clone()),
            },
        }
    }

    pub fn put(&mut self, key: InnerKey, value: Value) -> Result<(), io::Error> {
        let should_flush = self.vm.write(get_raw_value(&value)).unwrap();
        // TODO:
        // 1. insert keys
        // 2. update index
        if should_flush {
            self.vm.flush();
        }
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

pub struct ValueManager {
    buf: RwLock<MmapMut>,
    buf_pos: usize,
    file: RwLock<DirectFile>,
    file_pos: usize,
}

impl ValueManager {
    pub fn new<P: AsRef<Path>>(db_file: P) -> Self {
        let mmap_buffer = get_rw_mmap_fd(&db_file, BUFFER_SIZE, KEY_FILE_SIZE as u64);
        let buf_pos = util::get_pos_of_buffer(&mmap_buffer).unwrap();

        let direct_file =
            DirectFile::open(&db_file, Mode::Append, FileAccess::ReadWrite, BUFFER_SIZE).unwrap();
        let file_pos = direct_file.end_pos();

        ValueManager {
            buf: RwLock::new(mmap_buffer),
            buf_pos,
            file: RwLock::new(direct_file),
            file_pos,
        }
    }

    pub fn write(&mut self, buf: &[u8]) -> Result<bool, error::Error> {
        if buf.len() != VALUE_SIZE {
            return Err(error::Error::InvalidValueSize);
        }
        let mut wbuf = self.buf.write().unwrap();

        let mut index = 0;

        while index < VALUE_SIZE {
            wbuf[self.buf_pos + index] = buf[index];
            index += 1;
        }

        self.buf_pos += VALUE_SIZE;

        Ok(self.buf_pos >= BUFFER_SIZE)
    }

    pub fn flush(&mut self) {
        // Do flush
        let mut wbuf = self.buf.write().unwrap();
        let wfile = self.file.write().unwrap();
        let bytes = wfile
            .pwrite(&wbuf, self.file_pos as u64)
            .expect("Failed to append to db file");
        self.file_pos += bytes;

        // Clear buffer
        wbuf.copy_from_slice(&[0u8; VALUE_SIZE]);
        self.buf_pos = 0;
    }

    pub fn read(&self, pos: usize) -> Result<Value, error::Error> {
        let mut offset = pos * VALUE_SIZE + KEY_FILE_SIZE + BUFFER_SIZE;
        if offset > self.file_pos + BUFFER_SIZE {
            return Err(error::Error::OutOfIndex);
        } else if offset > self.file_pos {
            let rbuf = self.buf.read().unwrap();
            offset -= self.file_pos;
            let data = &rbuf[offset..offset + VALUE_SIZE];
            return Ok(value_from_raw_bytes(data).unwrap());
        } else {
            // Read from dio
            let rfile = self.file.read().unwrap();
            let mut data = [0; VALUE_SIZE];
            rfile.pread(&mut data, offset as u64);
            return Ok(value_from_raw_bytes(&data).unwrap());
        }
    }
}

pub struct KeyManager {
    keys: RwLock<MmapMut>,
    index: RwLock<Vec<Key>>,
    pos: usize,
}

impl KeyManager {
    pub fn new<P: AsRef<Path>>(db_file: P) -> Self {
        let mmap_key = get_rw_mmap_fd(&db_file, KEY_FILE_SIZE, 0);
        let index = build_index(&mmap_key).unwrap();
        let pos = index.len();

        KeyManager {
            keys: RwLock::new(mmap_key),
            index: RwLock::new(index),
            pos,
        }
    }

    pub fn find(&self, inner: &InnerKey) -> Option<Key> {
        let rindex = self.index.read().unwrap();
        let kentry = bsearch(&*rindex, &inner);
        match kentry {
            None => None,
            Some(entry) => Some(rindex[entry].clone()),
        }
    }
}
