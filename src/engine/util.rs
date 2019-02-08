use super::error::*;
use super::kv::*;

use memmap::{MmapMut, MmapOptions};
use std::fs::{self, File, OpenOptions};
use std::io::{BufReader, Read, Seek, SeekFrom};
use std::path::Path;

/// Binary search
/// Given an `InnerKey`
/// Returns the position of the key in the index vector
/// Returns `None` if not found
pub fn bsearch(index: &Vec<Key>, key: &InnerKey) -> Option<usize> {
    if index.len() == 0 {
        return None;
    }
    let mut left = 0;
    let mut right = index.len();
    while left <= right {
        let mut mid = left + (right - left) / 2;
        if mid >= index.len() {
            break;
        }
        if &index[mid].inner < key {
            left = mid + 1;
        } else if &index[mid].inner > key {
            right = mid - 1;
        } else {
            while mid < index.len() - 1 {
                if &index[mid + 1].inner == key {
                    mid += 1;
                } else {
                    break;
                }
            }
            return Some(mid);
        }
    }
    None
}

/// Binary search
/// Given an `InnerKey`
/// Returns a tuple in format (found, position)
/// When new kv pair inserted, find the index position and insert to it
pub fn find_insert_point(index: &Vec<Key>, key: &InnerKey) -> (bool, usize) {
    if index.len() == 0 {
        return (false, 0);
    }
    if key.raw < index[0].inner.raw {
        return (false, 0);
    }
    if key.raw > index[index.len() - 1].inner.raw {
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
        if &index[mid].inner < key {
            left = mid + 1;
        } else if &index[mid].inner > key {
            if &index[mid - 1].inner < key {
                return (false, mid);
            }
            right = mid - 1;
        } else {
            if mid == index.len() - 1 {
                return (true, mid + 1);
            }
            while mid < index.len() - 1 {
                if &index[mid + 1].inner == key {
                    mid += 1;
                } else {
                    break;
                }
            }
            return (true, mid);
        }
    }
    (false, mid)
}

/// Get File with rw permission
pub fn get_rw_fd<P: AsRef<Path>>(file: P) -> File {
    OpenOptions::new()
        .read(true)
        .write(true)
        .open(&file)
        .expect(&format!("failed to open file: {:?}", file.as_ref()))
}

/// Get the mutable memmap handle
pub fn get_rw_mmap_fd<P: AsRef<Path>>(file: P, size: usize, offset: u64) -> MmapMut {
    let fd = get_rw_fd(file.as_ref());
    unsafe {
        MmapOptions::new()
            .len(size)
            .offset(offset)
            .map_mut(&fd)
            .expect(&format!("failed to mmap file: {:?}", file.as_ref()))
    }
}

/// Build index from keys file
/// step 1, load keys from &[u8]
/// step 2, multi-level sort keys by key and ventry number
/// for exmaple:
/// ```rust
/// [
///    // KEY( 8 bytes)       + ventry( 4 bytes)
///    2, 1, 1, 1, 1, 1, 1, 1,  0, 0, 0, 0, // the first record
///    1, 1, 1, 1, 1, 1, 1, 2,  0, 0, 0, 1, // the second record
///    1, 1, 1, 1, 1, 1, 1, 3,  0, 0, 0, 2, // the third record
///    2, 1, 1, 1, 1, 1, 1, 1,  0, 0, 0, 3, // the fourth record
/// ];
/// // ventries should be ordered as: [1, 2, 0, 3]
/// ```
pub fn build_index<P: AsRef<Path>>(path: P, start: u64, end: u64) -> Result<Vec<Key>, Error> {
    if (end - start) % KEY_FILE_SIZE as u64 != 0 {
        return Err(Error::WrongAlignment);
    }
    let file = File::open(path)?;
    let mut reader = BufReader::new(file);
    let mut v = Vec::new();
    for pos in (start..end).step_by(KEY_FILE_SIZE + VALUE_FILE_SIZE) {
        // For each chunk
        let mut mkey = vec![0; KEY_FILE_SIZE];
        reader.seek(SeekFrom::Start(pos as u64))?;
        reader.read_exact(&mut mkey)?;
        for x in (0..mkey.len()).step_by(MKEY_SIZE) {
            let chunk = &mkey[x..x + MKEY_SIZE];
            if chunk == [0; MKEY_SIZE] {
                break;
            }
            let mut inner_key = InnerKey { raw: [0; KEY_SIZE] };
            inner_key.raw.clone_from_slice(&chunk[0..KEY_SIZE]);
            v.push(Key {
                inner: inner_key,
                ventry: (chunk[KEY_SIZE] as usize) << 24
                    | (chunk[KEY_SIZE + 1] as usize) << 16
                    | (chunk[KEY_SIZE + 2] as usize) << 8
                    | chunk[KEY_SIZE + 3] as usize,
            });
        }
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

pub fn get_buffer_pos(buffer: &[u8]) -> Result<u64, Error> {
    if buffer.len() % VALUE_SIZE != 0 {
        return Err(Error::WrongAlignment);
    }
    let mut pos = 0;
    while pos < buffer.len() {
        let chunk = &buffer[pos..pos + VALUE_SIZE];
        if chunk[..] == [0; VALUE_SIZE][..] {
            break;
        }
        pos += VALUE_SIZE;
    }
    Ok(pos as u64)
}

pub fn ensure_size<P: AsRef<Path>>(path: P, chunk_size: u64, item_size: u64) -> Result<u64, Error> {
    let metadata = fs::metadata(&path);
    match metadata {
        Err(_) => {
            // Create
            File::create(&path)?;
            return ensure_size(path, chunk_size, item_size);
        }
        Ok(meta) => {
            let len = meta.len();
            let f = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .open(&path)?;
            if len == 0 {
                f.set_len(chunk_size)?;
                return Ok(0);
            } else {
                // Read last item
                let mut reader = BufReader::new(&f);
                reader.seek(SeekFrom::End(-(item_size as i64)))?;
                let mut buf = vec![0; item_size as usize];
                reader.read_exact(&mut buf)?;
                if buf[..] != vec![0; item_size as usize][..] {
                    // Full
                    f.set_len(len + chunk_size)?;
                    return Ok(len);
                }
                let pos = find_last_pos(&mut reader, len, chunk_size, item_size)?;
                return Ok(pos);
            }
        }
    }
}

fn find_last_pos<R: Read + Seek>(
    reader: &mut R,
    len: u64,
    chunk_size: u64,
    item_size: u64,
) -> Result<u64, Error> {
    let pos = len - chunk_size as u64;
    reader.seek(SeekFrom::Start(pos))?;
    let mut buf = vec![0; chunk_size as usize];
    reader.read_exact(&mut buf)?;
    // BUFFER_SIZE(4kb) is 16 times of VALUE_SIZE(256byte)
    for i in (0..chunk_size as usize).step_by(item_size as usize) {
        let chunk = &buf[i..i + item_size as usize];
        if chunk[..] == vec![0; item_size as usize][..] {
            return Ok(i as u64 + pos);
        }
    }
    unreachable!();
}

pub fn get_file_size<P: AsRef<Path>>(path: P) -> Result<u64, Error> {
    let metadata = fs::metadata(&path)?;
    Ok(metadata.len())
}

#[cfg(test)]
mod util_tests {
    #[cfg(test)]
    mod search_tests {
        use super::super::super::kv::*;
        use super::super::{bsearch, find_insert_point};

        #[test]
        fn bsearch_test() {
            let cases = [
                (vec![], "key001", None),
                (
                    vec!["key001", "key001", "key002", "key003"],
                    "key001",
                    Some(1),
                ),
                (vec!["key001", "key001", "key002", "key003"], "key004", None),
            ];
            for case in cases.iter() {
                let mut index: Vec<Key> = Vec::new();
                for i in 0..case.0.len() {
                    index.push(Key {
                        inner: case.0[i].parse().unwrap(),
                        ventry: i,
                    });
                }
                let result = bsearch(&index, &case.1.parse().unwrap());
                assert_eq!(result, case.2);
            }
        }

        #[test]
        fn find_insert_point_test() {
            let cases = [
                (vec![], "key001", (false, 0)),
                (
                    vec!["key001", "key001", "key002", "key003"],
                    "key001",
                    (true, 1),
                ),
                (
                    vec!["key001", "key001", "key002", "key003"],
                    "key004",
                    (false, 4),
                ),
            ];
            for case in cases.iter() {
                let mut index: Vec<Key> = Vec::new();
                for i in 0..case.0.len() {
                    index.push(Key {
                        inner: case.0[i].parse().unwrap(),
                        ventry: i,
                    });
                }
                let result = find_insert_point(&index, &case.1.parse().unwrap());
                assert_eq!(result, case.2);
            }
        }
    }

    #[cfg(test)]
    mod build_index_tests {
        use super::super::super::kv::*;
        use super::super::build_index;

        use std::fs::File;
        use std::io::Write;
        use std::path::PathBuf;
        use tempfile::tempdir;

        fn tmp_path(name: &str) -> PathBuf {
            let tmp = tempdir().unwrap();
            let mut path = tmp.into_path();

            path.push(name);
            path
        }

        #[test]
        fn broken_test() {
            let tmp_path = tmp_path("broken_test");
            File::create(&tmp_path).unwrap();
            let index = build_index(&tmp_path, 0, 11);
            assert!(index.is_err());
        }

        #[test]
        fn valid_test() {
            let data = [
                2, 1, 1, 1, 1, 1, 1, 1, 0, 0, 0, 0, // the first record
                1, 1, 1, 1, 1, 1, 1, 2, 0, 0, 0, 1, // the second record
                1, 1, 1, 1, 1, 1, 1, 3, 0, 0, 0, 2, // the third record
                2, 1, 1, 1, 1, 1, 1, 1, 0, 0, 0, 3, // the fourth record
            ];
            let tmp_path = tmp_path("broken_test");
            let mut f = File::create(&tmp_path).unwrap();
            f.write(&data).unwrap();
            f.write(&vec![0; KEY_FILE_SIZE - data.len()]).unwrap();
            let index = build_index(&tmp_path, 0, KEY_FILE_SIZE as u64).unwrap();
            // ventry should be ordered as: 1, 2, 0, 3
            let entries: Vec<usize> = index.iter().map(|key| key.ventry).collect();
            assert_eq!(entries, [1, 2, 0, 3]);
        }
    }

    use super::super::error::*;
    use super::super::kv::*;
    use super::get_buffer_pos;
    #[test]
    fn get_buffer_pos_test() {
        // Ok
        let cases = [
            (vec![], 0),
            (vec![0; VALUE_SIZE], 0),
            (vec![1; VALUE_SIZE], VALUE_SIZE),
            (
                [&[1; VALUE_SIZE][..], &[0; VALUE_SIZE]].concat(),
                VALUE_SIZE,
            ),
        ];

        for case in &cases {
            let result = get_buffer_pos(&case.0).unwrap();
            assert_eq!(result, case.1 as u64);
        }

        // Err
        let err = get_buffer_pos(&vec![1]).err().unwrap();
        assert_eq!(err, Error::WrongAlignment);
    }
}
