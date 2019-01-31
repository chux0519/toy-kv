use std::cmp::Ordering;
use std::io;
use std::iter::Iterator;

type KeyRaw = [u8; 8];
type ValueRaw = [u8; 256];

pub enum Value {
    Value(ValueRaw),
    Invalid,
}

#[derive(Debug)]
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
    index: usize,
}

impl<'a> StoreIter<'a> {
    pub fn new(store: &'a Store) -> Self {
        StoreIter { store, index: 0 }
    }
}

impl<'a> Iterator for StoreIter<'a> {
    type Item = (KeyRaw, ValueRaw);

    fn next(&mut self) -> Option<Self::Item> {
        dbg!(self.index);
        if self.index < self.store.index.len() {
            let key = &self.store.index[self.index];
            let ventry = key.ventry;
            self.index += 1;
            if let Value::Value(value) = self.store.values[ventry] {
                return Some((key.key, value));
            }
        }
        None
    }
}

fn bsearch(index: &Vec<Key>, key: &KeyRaw) -> Option<usize> {
    if index.len() == 0 {
        return None;
    }
    let mut left = 0;
    let mut right = index.len();
    let mut mid = left + (right - left) / 2;
    while left <= right {
        mid = left + (right - left) / 2;
        if &index[mid].key < key {
            left = mid + 1;
        } else if &index[mid].key > key {
            right = mid - 1;
        } else {
            return Some(mid);
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

    pub fn get(&self, key: &KeyRaw) -> Option<&ValueRaw> {
        match bsearch(&self.index, key) {
            None => return None,
            Some(pos) => {
                let k = &self.index[pos];
                if k.ventry < self.values.len() {
                    let v = &self.values[k.ventry];
                    match v {
                        Value::Invalid => return None,
                        Value::Value(val) => return Some(val),
                    }
                }
            }
        }
        None
    }

    pub fn put(&mut self, key: KeyRaw, value: Value) -> Result<(), io::Error> {
        // TODO: insert thread safelly
        let ventry = self.values.len();
        self.keys.push(Key {
            key: key.clone(),
            ventry,
        });
        self.values.push(value);
        // update index
        let (found, pos) = find_insert_point(&self.index, key.clone());
        if found {
            self.index[pos].ventry = ventry;
        } else {
            // dbg!(&pos);
            // dbg!(&self.index.len());
            if pos == self.index.len() {
                self.index.push(Key {
                    key: key.clone(),
                    ventry,
                });
            } else {
                self.index.insert(
                    pos,
                    Key {
                        key: key.clone(),
                        ventry,
                    },
                );
            }
        }
        dbg!(&self.index);
        Ok(())
    }

    pub fn delete(&mut self, key: KeyRaw) -> Result<(), io::Error> {
        self.put(key, Value::Invalid)
    }

    pub fn scan(&self) -> StoreIter {
        StoreIter::new(self)
    }
}

fn find_insert_point(index: &Vec<Key>, rkey: KeyRaw) -> (bool, usize) {
    if index.len() == 0 {
        return (false, 0);
    }
    if rkey < index[0].key {
        return (false, 0);
    }
    if rkey > index[index.len() - 1].key {
        return (false, index.len());
    }
    let mut left = 0;
    let mut right = index.len();
    let mut mid = left + (right - left) / 2;

    while left <= right {
        mid = left + (right - left) / 2;
        if mid == index.len() {
            break;
        }
        if &index[mid].key < &rkey {
            left = mid + 1;
        } else if &index[mid].key > &rkey {
            if &index[mid - 1].key < &rkey {
                return (false, mid);
            }
            right = mid - 1;
        } else {
            return (true, mid);
        }
    }
    (false, mid)
}
