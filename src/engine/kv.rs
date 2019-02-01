use std::cmp::Ordering;

/// Keys are only allowed in 8 bytes
/// While Values for 256 bytes each
/// See README.md#limitation
pub type KeyRaw = [u8; 8];
pub type ValueRaw = [u8; 256];

#[derive(Debug)]
pub struct Key {
    pub key: KeyRaw,
    pub ventry: usize,
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

pub enum Value {
    Valid(ValueRaw),
    Invalid,
}
