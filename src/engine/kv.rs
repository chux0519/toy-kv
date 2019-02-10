use std::cmp::Ordering;
use std::str::FromStr;
use std::string::ToString;

use super::error::Error;

/// key: 8 bytes
pub const KEY_SIZE: usize = 8;
/// 8 bytes for key, 4 bytes for the index of values
pub const MKEY_SIZE: usize = 12;
// each chunk max size
pub const MAX_KV_PAIR: usize = 65536;
/// size of keys which would be mem mapped
pub const KEY_FILE_SIZE: usize = MAX_KV_PAIR * MKEY_SIZE;

/// value: 256 bytes
pub const VALUE_SIZE: usize = 256;
/// size of each value block
pub const VALUE_FILE_SIZE: usize = MAX_KV_PAIR * VALUE_SIZE;

/// 16mb buffer size (mem mapped)
pub const BUFFER_SIZE: usize = 16 * 1024 * 1024;

/// Keys are only allowed in 8 bytes
/// While Values for 256 bytes each
/// See README.md#limitation
pub type KeyRaw = [u8; 8];
pub type ValueRaw = [u8; 256];

#[derive(Debug, Clone)]
pub struct InnerKey {
    pub raw: KeyRaw,
}

#[derive(Clone)]
pub struct InnerValue {
    pub raw: ValueRaw,
}

impl ToString for InnerKey {
    fn to_string(&self) -> String {
        self.raw
            .iter()
            .cloned()
            .map(|x| x as char)
            .collect::<String>()
            .trim_matches(char::from(0))
            .to_owned()
    }
}

impl ToString for InnerValue {
    fn to_string(&self) -> String {
        self.raw
            .iter()
            .cloned()
            .map(|x| x as char)
            .collect::<String>()
            .trim_matches(char::from(0))
            .to_owned()
    }
}

impl FromStr for InnerKey {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.len() > 8 {
            return Err(Error::ContentExceed);
        }
        let mut key = [0; 8];
        let chars = s.as_bytes();
        key[..s.len()].clone_from_slice(&chars[..s.len()]);
        Ok(InnerKey { raw: key })
    }
}

impl FromStr for InnerValue {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.len() > 256 {
            return Err(Error::ContentExceed);
        }
        let mut value = [0; 256];
        let chars = s.as_bytes();
        value[..s.len()].clone_from_slice(&chars[..s.len()]);
        Ok(InnerValue { raw: value })
    }
}

#[derive(Debug, Clone)]
pub struct Key {
    pub inner: InnerKey,
    pub ventry: usize,
}

pub enum Value {
    Valid(Box<InnerValue>),
    Invalid,
}

pub fn value_to_bytes(value: &Value) -> &[u8] {
    match value {
        Value::Invalid => &[255u8; VALUE_SIZE],
        Value::Valid(v) => &v.raw,
    }
}

pub fn value_from_bytes(bytes: &[u8]) -> Result<Value, Error> {
    if bytes.len() != VALUE_SIZE {
        return Err(Error::InvalidValueSize);
    }
    if bytes[..] == [255u8; VALUE_SIZE][..] {
        return Ok(Value::Invalid);
    }
    let mut inner = InnerValue {
        raw: [0; VALUE_SIZE],
    };
    inner.raw.clone_from_slice(bytes);
    Ok(Value::Valid(Box::new(inner)))
}

pub fn key_to_bytes(key: &Key) -> Vec<u8> {
    let mut bytes = vec![0u8; MKEY_SIZE];
    bytes[..KEY_SIZE].clone_from_slice(&key.inner.raw[..KEY_SIZE]);
    let ventry = key.ventry;
    bytes[KEY_SIZE] = (ventry >> 24) as u8;
    bytes[KEY_SIZE + 1] = (ventry >> 16) as u8;
    bytes[KEY_SIZE + 2] = (ventry >> 8) as u8;
    bytes[KEY_SIZE + 3] = ventry as u8;

    bytes
}

impl PartialOrd for InnerKey {
    fn partial_cmp(&self, other: &InnerKey) -> Option<Ordering> {
        Some(self.raw.cmp(&other.raw))
    }
}

impl PartialEq for InnerKey {
    fn eq(&self, other: &InnerKey) -> bool {
        self.raw == other.raw
    }
}
