use super::dio::{Block4k, DirectFile, FileAccess, Mode};
use super::error;
use super::kv::*;
use super::util::{self, *};

use std::path::{Path, PathBuf};
use std::sync::RwLock;

use memmap::MmapMut;

/// Seperating keys and values
/// Managing keys and index via km
/// Managing values via vm
pub struct Store {
    km: KeyManager,
    vm: ValueManager,
    key_file: PathBuf,
    buffer_file: PathBuf,
    value_file: PathBuf,
}

/// For iteraing the store
pub struct StoreIter<'a> {
    store: &'a mut Store,
    index: usize,
    end: u32,
}

impl<'a> StoreIter<'a> {
    pub fn new(store: &'a mut Store, start: u32, end: u32) -> Self {
        StoreIter {
            store,
            index: start as usize,
            end,
        }
    }
}

impl<'a> Iterator for StoreIter<'a> {
    type Item = (InnerKey, InnerValue);

    fn next(&mut self) -> Option<Self::Item> {
        let rindex = self.store.km.index.read().unwrap();
        while self.index < rindex.len() && self.index < self.end as usize {
            let key = &rindex[self.index];
            if self.index + 1 < rindex.len() && key.inner == rindex[self.index + 1].inner {
                self.index += 1;
                continue;
            }
            let value = self.store.vm.read(key.ventry).unwrap();
            self.index += 1;
            if let Value::Valid(v) = value {
                return Some((key.inner.clone(), *v.clone()));
            }
        }
        None
    }
}

impl Store {
    pub fn new<P: AsRef<Path>>(
        key_file: P,
        value_file: P,
        buffer_file: P,
    ) -> Result<Self, error::Error> {
        // Make sure the DB files have enough space
        let key_pos = util::ensure_size(&key_file, KEY_FILE_SIZE as u64, MKEY_SIZE as u64)?;
        util::ensure_size(&value_file, VALUE_FILE_SIZE as u64, VALUE_SIZE as u64)?;
        // FIXME: Corner case should do a flush when the buffer is full here
        let buffer_pos = util::ensure_size(&buffer_file, BUFFER_SIZE as u64, VALUE_SIZE as u64)?;

        // Compute the value file position
        let value_pos =
            (key_pos / MKEY_SIZE as u64 - buffer_pos / VALUE_SIZE as u64) * VALUE_SIZE as u64;
        Store::init(&key_file, &value_file, &buffer_file, value_pos)
    }

    fn ensure_size(&mut self) -> Result<(u64, u64, u64), error::Error> {
        let key_pos = util::ensure_size(&self.key_file, KEY_FILE_SIZE as u64, MKEY_SIZE as u64)?;
        util::ensure_size(&self.value_file, VALUE_FILE_SIZE as u64, VALUE_SIZE as u64)?;
        let buffer_pos =
            util::ensure_size(&self.buffer_file, BUFFER_SIZE as u64, VALUE_SIZE as u64)?;

        let value_pos =
            (key_pos / MKEY_SIZE as u64 - buffer_pos / VALUE_SIZE as u64) * VALUE_SIZE as u64;

        let mmap_key = get_rw_mmap_fd(&self.key_file, KEY_FILE_SIZE, key_pos);
        self.km.keys = RwLock::new(mmap_key);
        Ok((key_pos, buffer_pos, value_pos))
    }

    fn init<P: AsRef<Path>>(
        key_file: P,
        value_file: P,
        buffer_file: P,
        value_pos: u64,
    ) -> Result<Self, error::Error> {
        // Init buffer(mmap)
        let mmap_buffer = get_rw_mmap_fd(&buffer_file, BUFFER_SIZE, 0);
        let buf_pos = util::get_buffer_pos(&mmap_buffer)?;

        // Get values(dio) handle
        let direct_file = DirectFile::open(&value_file, Mode::Open, FileAccess::ReadWrite, 4096)?;

        // Build index
        let key_file_end = util::get_file_size(&key_file)?;
        let index = build_index(&key_file, 0, key_file_end)?;
        let ventry = index.len();

        // Init keys(mmap)
        let mmap_key = get_rw_mmap_fd(
            &key_file,
            KEY_FILE_SIZE,
            key_file_end - KEY_FILE_SIZE as u64,
        );

        let km = KeyManager::new(mmap_key, index, ventry);

        let vm = ValueManager::new(mmap_buffer, buf_pos, direct_file, value_pos);

        Ok(Store {
            km,
            vm,
            key_file: key_file.as_ref().to_path_buf(),
            buffer_file: buffer_file.as_ref().to_path_buf(),
            value_file: value_file.as_ref().to_path_buf(),
        })
    }

    pub fn get(&mut self, key: InnerKey) -> Result<Option<InnerValue>, error::Error> {
        let key = self.km.find(&key);
        match key {
            None => Ok(None),
            Some(k) => match self.vm.read(k.ventry)? {
                Value::Invalid => Ok(None),
                Value::Valid(val) => Ok(Some(*val.clone())),
            },
        }
    }

    pub fn put(&mut self, key: InnerKey, value: Value) -> Result<(), error::Error> {
        // Write to buffer
        let should_flush = self.vm.write(value_to_bytes(&value))?;

        // Update keys and index
        self.km.put(&key);

        // Check should flush to disk or not
        if should_flush {
            // Flush to disk
            let file_pos = self.vm.flush();
            // Check if need more space
            if file_pos % VALUE_FILE_SIZE as u64 == 0 {
                self.ensure_size()?;
            }
        }

        Ok(())
    }

    pub fn delete(&mut self, key: InnerKey) -> Result<(), error::Error> {
        self.put(key, Value::Invalid)
    }

    pub fn scan(&mut self, start: u32, end: u32) -> StoreIter {
        StoreIter::new(self, start, end)
    }
}

pub struct ValueManager {
    buf: RwLock<MmapMut>,
    buf_pos: u64,
    file: RwLock<DirectFile>,
    file_pos: u64,
    cache: PageCache,
}

impl ValueManager {
    pub fn new(mmap_buffer: MmapMut, buf_pos: u64, direct_file: DirectFile, file_pos: u64) -> Self {
        ValueManager {
            buf: RwLock::new(mmap_buffer),
            buf_pos,
            file: RwLock::new(direct_file),
            file_pos,
            cache: PageCache::new(),
        }
    }

    pub fn write(&mut self, buf: &[u8]) -> Result<bool, error::Error> {
        if buf.len() != VALUE_SIZE {
            return Err(error::Error::InvalidValueSize);
        }
        let mut wbuf = self.buf.write().unwrap();

        let mut index = 0;

        while index < VALUE_SIZE {
            wbuf[self.buf_pos as usize + index] = buf[index];
            index += 1;
        }

        self.buf_pos += VALUE_SIZE as u64;

        Ok(self.buf_pos >= BUFFER_SIZE as u64)
    }

    pub fn flush(&mut self) -> u64 {
        // Do flush
        let mut wbuf = self.buf.write().unwrap();
        let wfile = self.file.write().unwrap();
        // wbuf must be a multiple of the page size(512 kb)
        let bytes = wfile
            .pwrite(&wbuf, self.file_pos as u64)
            .expect("Failed to append to db file");
        self.file_pos += bytes as u64;

        // Clear buffer
        wbuf.copy_from_slice(&[0u8; BUFFER_SIZE]);
        self.buf_pos = 0;
        self.file_pos
    }

    pub fn read(&mut self, ventry: usize) -> Result<Value, error::Error> {
        let mut offset = ventry * VALUE_SIZE;
        if offset as u64 > self.file_pos + BUFFER_SIZE as u64 {
            Err(error::Error::OutOfIndex)
        } else if offset as u64 >= self.file_pos {
            // Value is in buffer
            let rbuf = self.buf.read().unwrap();
            offset -= self.file_pos as usize;
            let data = &rbuf[offset..offset + VALUE_SIZE];
            Ok(value_from_bytes(data).unwrap())
        } else {
            // Value is in file
            // try get value from page cache here
            let v = self.cache.try_get(offset as u64, VALUE_SIZE as u64);
            match v {
                None => {
                    // Read from dio
                    let rfile = self.file.read().unwrap();
                    let bytes = self
                        .cache
                        .try_load(&rfile, VALUE_SIZE as u64, offset as u64)?;
                    Ok(value_from_bytes(&bytes).unwrap())
                }
                Some(bytes) => Ok(value_from_bytes(&bytes).unwrap()),
            }
        }
    }
}

/// Read 4k block values as cache
struct PageCache {
    cache: Block4k,
    start: u64,
    end: u64,
}

impl PageCache {
    pub fn new() -> Self {
        PageCache {
            cache: Block4k { bytes: [0; 4096] },
            start: 0,
            end: 0,
        }
    }
    pub fn try_load(
        &mut self,
        dio_file: &DirectFile,
        len: u64,
        offset: u64,
    ) -> Result<Vec<u8>, error::Error> {
        const PAGE_SIZE: u64 = 512;
        const CACHE_SIZE: u64 = 4096;
        let md = offset % PAGE_SIZE;
        if len - md > CACHE_SIZE {
            return Err(error::Error::CacheTooSmall);
        }
        let read = dio_file.pread(&mut self.cache.bytes, offset - md)?;
        self.start = offset - md;
        self.end = self.start + read;
        let v = self.try_get(offset, len).unwrap();
        Ok(v)
    }
    pub fn try_get(&self, offset: u64, len: u64) -> Option<Vec<u8>> {
        if offset >= self.start && offset + len <= self.end {
            let pos = (offset - self.start) as usize;
            return Some(Vec::from(&self.cache.bytes[pos..pos + len as usize]));
        }
        None
    }
}

pub struct KeyManager {
    keys: RwLock<MmapMut>,
    index: RwLock<Vec<Key>>,
    ventry: usize,
}

impl KeyManager {
    pub fn new(mmap_key: MmapMut, index: Vec<Key>, ventry: usize) -> Self {
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

        let ventry = windex.len();
        let new_key = Key {
            inner: key.clone(),
            ventry,
        };
        let kbytes = key_to_bytes(&new_key);

        // Update index
        let (_found, pos) = find_insert_point(&windex, key);

        if pos == windex.len() {
            windex.push(new_key);
        } else {
            windex.insert(pos, new_key);
        }

        // Append to keys (mmap)
        let offset = ventry % MAX_KV_PAIR * MKEY_SIZE;

        for pos in offset..offset + MKEY_SIZE {
            wkeys[pos] = kbytes[pos - offset];
        }

        self.ventry = windex.len();
    }
}
