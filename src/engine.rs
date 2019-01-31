use std::cmp::Ordering;
use std::io;
use std::iter::Iterator;

type KeyRaw = [u8; 8];
type ValueRaw = [u8; 256];

pub enum Value {
    Value(ValueRaw),
    Invalid,
}

pub struct Key {
    key: KeyRaw,
    ventry: usize,
}

impl PartialOrd for Key {
    fn partial_cmp(&self, other: &Key) -> Option<Ordering> {
        Some(self.key.cmp(&other.key))
    }
}

impl PartialEq for Key {
    fn eq(&self, other: &Key) -> bool {
        self.key == other.key
    }
}

pub struct Store {
    keys: Vec<Key>,
    values: Vec<Value>,
    index: Vec<Key>,
}

pub struct StoreIter<'a> {
    store: &'a Store,
}

impl<'a> StoreIter<'a> {
    pub fn new(store: &'a Store) -> Self {
        StoreIter { store }
    }
}

impl<'a> Iterator for StoreIter<'a> {
    type Item = (KeyRaw, ValueRaw);

    fn next(&mut self) -> Option<Self::Item> {
        // TODO:
        None
    }
}

fn bsearch<'a>(index: &'a Vec<Key>, key: &Key) -> Option<&'a Key> {
    if index.len() == 0 {
        return None;
    }
    let mut left = 0;
    let mut right = index.len();
    let mut mid = left + (right - left) / 2;
    while left <= right {
        mid = left + (right - left) / 2;
        if &index[mid] < key {
            left = mid + 1;
        } else if &index[mid] > key {
            right = mid - 1;
        } else {
            return Some(&index[mid]);
        }
    }
    None
}

impl Store {
    pub fn new() -> Self {
        Store {
            keys: Vec::new(),
            values: Vec::new(),
            index: Vec::new(),
        }
    }

    pub fn Get(&self, key: &Key) -> Option<&Value> {
        let key = bsearch(&self.index, key);
        match key {
            None => return None,
            Some(k) => {
                if k.ventry < self.values.len() {
                    return Some(&self.values[k.ventry]);
                }
            }
        }
        None
    }

    pub fn Put(&mut self, key: KeyRaw, value: ValueRaw) -> Result<(), io::Error> {
        // TODO: insert thread safelly
        Ok(())
    }

    pub fn Delete(&mut self, key: KeyRaw) -> Result<(), io::Error> {
        Ok(())
    }

    pub fn Scan(&self) -> StoreIter {
        StoreIter::new(self)
    }
}
