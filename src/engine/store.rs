use super::dio::{DirectFile, FileAccess, Mode};
use super::error;
use super::kv::*;
use super::util::{self, *};

use std::io;
use std::path::{Path, PathBuf};
use std::sync::RwLock;

use memmap::MmapMut;

/// Seperating keys and values
/// both of them are insert only vector
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
        let rindex = self.store.km.index.read().unwrap();
        while self.index < rindex.len() {
            let key = &rindex[self.index];
            if self.index + 1 < rindex.len() && key.inner == rindex[self.index + 1].inner {
                self.index += 1;
                continue;
            }
            let value = self.store.vm.read(key.ventry).unwrap();
            self.index += 1;
            if let Value::Valid(v) = value {
                return Some((key.inner.clone(), v.clone()));
            }
        }
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
        // Write to buffer
        let should_flush = self.vm.write(value_to_bytes(&value)).unwrap();

        // Update keys and index
        self.km.put(&key);

        // Check should flush to disk or not
        if should_flush {
            self.vm.flush();
        }

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

    pub fn read(&self, ventry: usize) -> Result<Value, error::Error> {
        let mut offset = ventry * VALUE_SIZE;
        let values_len = self.file_pos - KEY_FILE_SIZE - BUFFER_SIZE;
        if offset > values_len + BUFFER_SIZE {
            return Err(error::Error::OutOfIndex);
        } else if offset > values_len {
            let rbuf = self.buf.read().unwrap();
            offset -= values_len;
            let data = &rbuf[offset..offset + VALUE_SIZE];
            return Ok(value_from_bytes(data).unwrap());
        } else {
            // Read from dio
            let rfile = self.file.read().unwrap();
            let mut data = [0; VALUE_SIZE];
            rfile.pread(&mut data, (offset + KEY_FILE_SIZE + BUFFER_SIZE) as u64);
            return Ok(value_from_bytes(&data).unwrap());
        }
    }
}

pub struct KeyManager {
    keys: RwLock<MmapMut>,
    index: RwLock<Vec<Key>>,
    ventry: usize,
}

impl KeyManager {
    pub fn new<P: AsRef<Path>>(db_file: P) -> Self {
        let mmap_key = get_rw_mmap_fd(&db_file, KEY_FILE_SIZE, 0);
        let index = build_index(&mmap_key).unwrap();
        let ventry = index.len();

        KeyManager {
            keys: RwLock::new(mmap_key),
            index: RwLock::new(index),
            ventry,
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

    pub fn put(&mut self, key: &InnerKey) {
        let mut windex = self.index.write().unwrap();
        let mut wkeys = self.keys.write().unwrap();

        let ventry = self.ventry;
        let new_key = Key {
            inner: key.clone(),
            ventry,
        };
        let kbytes = key_to_bytes(&new_key);

        // Update index
        let (found, pos) = find_insert_point(&windex, key);
        dbg!(&found);
        dbg!(&pos);
        dbg!(windex.len());
        if pos == windex.len() {
            windex.push(new_key);
        } else {
            windex.insert(pos, new_key);
        }

        // Append to keys (mmap)
        let offset = ventry * MKEY_SIZE;

        for pos in offset..offset + MKEY_SIZE {
            wkeys[pos] = kbytes[pos - offset];
        }

        self.ventry += 1;
    }
}
