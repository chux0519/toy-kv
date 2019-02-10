use std::convert::From;
use std::fmt::{self, Debug, Display};
use std::io;

pub enum Error {
    // For converting string to key
    ContentExceed,
    // For alignment check
    WrongAlignment,
    // For checking bound when reading values
    OutOfIndex,
    // For build value from [u8]
    InvalidValueSize,
    // For checking cache size when trying to read value
    CacheTooSmall,
    // For io error
    IoError(io::Error),
}

impl PartialEq for Error {
    fn eq(&self, other: &Error) -> bool {
        format!("{}", self) == format!("{}", other)
    }
}

impl From<io::Error> for Error {
    fn from(err: io::Error) -> Self {
        Error::IoError(err)
    }
}

impl Debug for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Error::ContentExceed => write!(f, "Content too long"),
            Error::WrongAlignment => write!(f, "Alignment error"),
            Error::OutOfIndex => write!(f, "Read out of index"),
            Error::InvalidValueSize => write!(f, "Invalid value size"),
            Error::CacheTooSmall => write!(f, "Cache too small"),
            Error::IoError(err) => write!(f, "{:?}", err),
        }
    }
}

impl Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Error::ContentExceed => write!(f, "Content too long"),
            Error::WrongAlignment => write!(f, "Alignment error"),
            Error::OutOfIndex => write!(f, "Read out of index"),
            Error::InvalidValueSize => write!(f, "Invalid value size"),
            Error::CacheTooSmall => write!(f, "Cache too small"),
            Error::IoError(err) => write!(f, "{}", err),
        }
    }
}

impl From<Error> for io::Error {
    fn from(e: Error) -> io::Error {
        match e {
            Error::ContentExceed => io::Error::new(io::ErrorKind::Other, "Content too long"),
            Error::WrongAlignment => io::Error::new(io::ErrorKind::Other, "Alignment error"),
            Error::OutOfIndex => io::Error::new(io::ErrorKind::Other, "Read out of index"),
            Error::CacheTooSmall => io::Error::new(io::ErrorKind::Other, "Cache too small"),
            Error::InvalidValueSize => io::Error::new(io::ErrorKind::Other, "Invalid value size"),
            Error::IoError(err) => err,
        }
    }
}
