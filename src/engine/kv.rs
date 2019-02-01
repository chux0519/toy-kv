use std::cmp::Ordering;
use std::error;
use std::fmt;
use std::str::FromStr;
use std::string::ToString;

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
        self.raw.iter().cloned().map(|x| x as char).collect()
    }
}

impl ToString for InnerValue {
    fn to_string(&self) -> String {
        self.raw.iter().cloned().map(|x| x as char).collect()
    }
}

impl FromStr for InnerKey {
    type Err = LongContentError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.len() > 8 {
            return Err(LongContentError);
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
    type Err = LongContentError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.len() > 256 {
            return Err(LongContentError);
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
pub struct LongContentError;

impl fmt::Display for LongContentError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "content too long")
    }
}

impl error::Error for LongContentError {
    fn description(&self) -> &str {
        "content too long"
    }

    fn cause(&self) -> Option<&error::Error> {
        None
    }
}

#[derive(Debug)]
pub struct Key {
    pub inner: InnerKey,
    pub ventry: usize,
}

pub enum Value {
    Valid(InnerValue),
    Invalid,
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
