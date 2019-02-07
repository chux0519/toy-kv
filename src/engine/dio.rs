use std::os::unix::ffi::OsStrExt;
use std::os::unix::io::{FromRawFd, RawFd};

use std::fs::File;
use std::io::{self, Seek};
use std::path::Path;

use libc;

pub struct DirectFile {
    fd: RawFd,
    alignment: usize,
}

const O_DIRECT: i32 = 0x4000; // For Linux

#[inline]
fn retry<F: Fn() -> isize>(f: F) -> isize {
    loop {
        let n = f();
        if n != -1 || io::Error::last_os_error().kind() != io::ErrorKind::Interrupted {
            return n;
        }
    }
}

pub enum Mode {
    Open,
    Append,
    Truncate,
}

pub enum FileAccess {
    Read,
    Write,
    ReadWrite,
}

impl DirectFile {
    pub fn open<P: AsRef<Path>>(
        path: P,
        mode: Mode,
        fa: FileAccess,
        alignment: usize,
    ) -> io::Result<DirectFile> {
        let flags = O_DIRECT
            | match mode {
                Mode::Open => 0,
                Mode::Append => libc::O_APPEND,
                Mode::Truncate => libc::O_TRUNC,
            };
        // Opening with a write permission must silently create the file.
        let (flags, mode) = match fa {
            FileAccess::Read => (flags | libc::O_RDONLY, 0),
            FileAccess::Write => (
                flags | libc::O_WRONLY | libc::O_CREAT,
                libc::S_IRUSR | libc::S_IWUSR,
            ),
            FileAccess::ReadWrite => (
                flags | libc::O_RDWR | libc::O_CREAT,
                libc::S_IRUSR | libc::S_IWUSR,
            ),
        };
        let path = path.as_ref().as_os_str().as_bytes();
        match retry(|| unsafe {
            libc::open(path.as_ptr() as *const i8, flags, mode as libc::c_uint) as isize
        }) {
            -1 => Err(io::Error::last_os_error()),
            fd => Ok(DirectFile {
                fd: fd as i32,
                alignment: alignment,
            }),
        }
    }

    pub fn alignment(&self) -> usize {
        self.alignment
    }

    pub fn pread(&self, buf: &mut [u8], off: u64) -> io::Result<usize> {
        let r = unsafe {
            ::libc::pread(
                self.fd,
                buf.as_mut_ptr() as *mut libc::c_void,
                buf.len(),
                off as i64,
            )
        };

        if r < 0 {
            Err(io::Error::last_os_error())
        } else {
            Ok(r as usize)
        }
    }

    pub fn pwrite(&self, buf: &[u8], off: u64) -> io::Result<usize> {
        let r = unsafe {
            ::libc::pwrite(
                self.fd,
                buf.as_ptr() as *const libc::c_void,
                buf.len(),
                off as i64,
            )
        };

        if r < 0 {
            Err(io::Error::last_os_error())
        } else {
            Ok(r as usize)
        }
    }
}
