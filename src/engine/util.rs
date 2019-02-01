use super::kv::*;

/// Binary search, for `get` method
pub fn bsearch(index: &Vec<Key>, key: &KeyRaw) -> Option<usize> {
    if index.len() == 0 {
        return None;
    }
    let mut left = 0;
    let mut right = index.len();
    let mut mid = left + (right - left) / 2;
    while left <= right {
        mid = left + (right - left) / 2;
        if &index[mid].key < key {
            left = mid + 1;
        } else if &index[mid].key > key {
            right = mid - 1;
        } else {
            return Some(mid);
        }
    }
    None
}

/// Binary search the insert/update point
/// When new kv pair inserted, find the index position and insert / update.
pub fn find_insert_point(index: &Vec<Key>, rkey: KeyRaw) -> (bool, usize) {
    if index.len() == 0 {
        return (false, 0);
    }
    if rkey < index[0].key {
        return (false, 0);
    }
    if rkey > index[index.len() - 1].key {
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
        if &index[mid].key < &rkey {
            left = mid + 1;
        } else if &index[mid].key > &rkey {
            if &index[mid - 1].key < &rkey {
                return (false, mid);
            }
            right = mid - 1;
        } else {
            return (true, mid);
        }
    }
    (false, mid)
}
