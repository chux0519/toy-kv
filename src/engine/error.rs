use std::fmt::{self, Debug, Display};
use std::io;

pub enum Error {
    ContentTooLong,
    IndexFileBroken,
}

impl Debug for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            Error::ContentTooLong => write!(f, "Content too long"),
            Error::IndexFileBroken => write!(f, "Index File Broken"),
        }
    }
}

impl Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            Error::ContentTooLong => write!(f, "Content too long"),
            Error::IndexFileBroken => write!(f, "Index File Broken"),
        }
    }
}

impl From<Error> for io::Error {
    fn from(e: Error) -> io::Error {
        match e {
            Error::ContentTooLong => io::Error::new(io::ErrorKind::Other, "Content too long"),
            Error::IndexFileBroken => io::Error::new(io::ErrorKind::Other, "Index File Broken"),
        }
    }
}
