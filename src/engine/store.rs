use super::kv::*;
use super::util::*;

use std::io;
use std::sync::RwLock;

/// Seperating keys and values
/// `keys` and `values` are both insert only vector
/// `index` for ordering the keys in the store
pub struct Store {
    keys: RwLock<Vec<Key>>,
    values: RwLock<Vec<Value>>,
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
        if self.index < rindex.len() {
            let key = &rindex[self.index];
            let ventry = key.ventry;
            self.index += 1;
            if let Value::Valid(inner_value) = &rvalues[ventry] {
                return Some((key.inner.clone(), inner_value.clone()));
            }
        }
        None
    }
}

impl Store {
    pub fn new() -> Self {
        Store {
            keys: RwLock::new(Vec::new()),
            values: RwLock::new(Vec::new()),
            index: RwLock::new(Vec::new()),
        }
    }

    pub fn get(&self, key: InnerKey) -> Option<InnerValue> {
        let rindex = self.index.read().unwrap();
        let rvalues = self.values.read().unwrap();
        match bsearch(&*rindex, &key) {
            None => return None,
            Some(pos) => {
                let k = &rindex[pos];
                if k.ventry < rvalues.len() {
                    let v = &rvalues[k.ventry];
                    match v {
                        Value::Invalid => return None,
                        Value::Valid(val) => return Some(val.clone()),
                    }
                }
            }
        }
        None
    }

    pub fn put(&mut self, key: InnerKey, value: Value) -> Result<(), io::Error> {
        let mut windex = self.index.write().unwrap();
        let mut wkeys = self.keys.write().unwrap();
        let mut wvalues = self.values.write().unwrap();
        let ventry = wvalues.len();
        wkeys.push(Key {
            inner: key.clone(),
            ventry,
        });
        wvalues.push(value);
        // update index
        let (found, pos) = find_insert_point(&windex, &key);
        if found {
            windex[pos].ventry = ventry;
        } else {
            if pos == windex.len() {
                windex.push(Key {
                    inner: key.clone(),
                    ventry,
                });
            } else {
                windex.insert(
                    pos,
                    Key {
                        inner: key.clone(),
                        ventry,
                    },
                );
            }
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
