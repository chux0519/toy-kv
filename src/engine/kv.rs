use std::cmp::Ordering;
use std::str::FromStr;
use std::string::ToString;

use super::error::Error;

pub const KEY_SIZE: usize = 8;
pub const MKEY_SIZE: usize = 12; // 8 bytes for key, 4 bytes for the index of values
pub const VALUE_SIZE: usize = 256;
pub const KEY_FILE_SIZE: usize = 65536 * MKEY_SIZE;
pub const BUFFER_SIZE: usize = 4 * 1024; // 4kb buffer szie

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
        for i in 0..s.len() {
            key[i] = chars[i];
        }
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
        for i in 0..s.len() {
            value[i] = chars[i];
        }
        Ok(InnerValue { raw: value })
    }
}

#[derive(Debug, Clone)]
pub struct Key {
    pub inner: InnerKey,
    pub ventry: usize,
}

pub enum Value {
    Valid(InnerValue),
    Invalid,
}

pub fn get_raw_value(value: &Value) -> &[u8] {
    match value {
        Value::Invalid => &[255u8; VALUE_SIZE],
        Value::Valid(v) => &v.raw,
    }
}

pub fn value_from_raw_bytes(bytes: &[u8]) -> Result<Value, Error> {
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
    Ok(Value::Valid(inner))
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
