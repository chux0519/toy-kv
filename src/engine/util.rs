use super::error::*;
use super::kv::*;

use memmap::{MmapMut, MmapOptions};
use std::fs::{File, OpenOptions};
use std::path::PathBuf;

/// Binary search, for `get` method
pub fn bsearch(index: &Vec<Key>, key: &InnerKey) -> Option<usize> {
    if index.len() == 0 {
        return None;
    }
    let mut left = 0;
    let mut right = index.len();
    while left <= right {
        let mid = left + (right - left) / 2;
        if &index[mid].inner < key {
            left = mid + 1;
        } else if &index[mid].inner > key {
            right = mid - 1;
        } else {
            return Some(mid);
        }
    }
    None
}

/// Binary search the insert/update point
/// When new kv pair inserted, find the index position and insert / update.
pub fn find_insert_point(index: &Vec<Key>, rkey: &InnerKey) -> (bool, usize) {
    if index.len() == 0 {
        return (false, 0);
    }
    if rkey.raw < index[0].inner.raw {
        return (false, 0);
    }
    if rkey.raw > index[index.len() - 1].inner.raw {
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
        if &index[mid].inner < rkey {
            left = mid + 1;
        } else if &index[mid].inner > rkey {
            if &index[mid - 1].inner < rkey {
                return (false, mid);
            }
            right = mid - 1;
        } else {
            return (true, mid);
        }
    }
    (false, mid)
}

pub fn get_rw_fd(file: &PathBuf) -> File {
    OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .open(&file)
        .expect(&format!("failed to open file: {:?}", file))
}

pub fn get_rw_mmap_fd(file: PathBuf, size: usize) -> MmapMut {
    let fd = get_rw_fd(&file);

    unsafe {
        MmapOptions::new()
            .len(size)
            .map_mut(&fd)
            .expect(&format!("failed to mmap file: {:?}", file))
    }
}

/// Building index from keys file
/// 1. load keys from &[u8]
/// 2. sort keys by key and ventry number
pub fn build_index(mkey: &[u8]) -> Result<Vec<Key>, Error> {
    if mkey.len() % 12 != 0 {
        return Err(Error::IndexFileBroken);
    }
    let mut start = 0;
    let mut end = 12;
    let mut v = Vec::new();
    while end <= mkey.len() {
        let chunk = &mkey[start..end];
        if chunk == [0; 12] {
            println!("empty chunk detected, start at {}, end at {}", start, end);
            break;
        }
        let mut inner_key = InnerKey { raw: [0; 8] };
        inner_key.raw.clone_from_slice(&chunk[0..8]);
        v.push(Key {
            inner: inner_key,
            ventry: (chunk[8] as usize) << 24
                | (chunk[9] as usize) << 16
                | (chunk[10] as usize) << 8
                | chunk[11] as usize,
        });
        start += 12;
        end += 12;
    }
    // Multi-Level sort by [(inner, asc), (ventry. asc)]
    v.sort_by(|a, b| {
        if a.inner == b.inner {
            a.ventry.partial_cmp(&b.ventry).unwrap()
        } else {
            a.inner.partial_cmp(&b.inner).unwrap()
        }
    });
    Ok(v)
}
