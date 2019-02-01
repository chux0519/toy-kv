use super::kv::*;
use super::util::*;

use std::io;

/// Seperating keys and values
/// `keys` and `values` are both insert only vector
/// `index` for ordering the keys in the store
pub struct Store {
    keys: Vec<Key>,
    values: Vec<Value>,
    index: Vec<Key>,
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
