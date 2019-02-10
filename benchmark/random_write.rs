use toy_kv::engine::kv;
use toy_kv::transport::open_db_from;

use tempfile::tempdir;
use time::PreciseTime;

fn main() {
    let tmp = tempdir().unwrap();
    let mut db = open_db_from(&tmp.into_path()).unwrap();
    let start = PreciseTime::now();
    for _ in 0..100_000 {
        let k = gen_rand_string(8);
        let v = gen_rand_string(256);
        db.put(
            k.parse().unwrap(),
            kv::Value::Valid(Box::new(v.parse().unwrap())),
        )
        .unwrap();
    }
    let end = PreciseTime::now();
    println!(
        "{} seconds for put, thr: {:.3} mb/s",
        start.to(end),
        f64::from(100_000 * (256 + 8) / 1024 / 1024)
            / (start.to(end).num_nanoseconds().unwrap() as f64 / 1e9) as f64
    );
}

/// Generate random string effeciently
/// Ref: https://colobu.com/2018/09/02/generate-random-string-in-Go/
const LETTER_IDX_BITS: u64 = 6;
const LETTER_IDX_MASK: u64 = (1 << LETTER_IDX_BITS) - 1;
const LETTER_IDX_MAX: u64 = 64 / LETTER_IDX_BITS;

pub fn gen_rand_string(n: usize) -> String {
    let letters: Vec<char> = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
        .to_owned()
        .chars()
        .collect();
    let mut b = vec!['x'; n];
    let mut i = (n - 1) as i64;
    let mut cache = rand::random::<u64>();
    let mut remain = LETTER_IDX_MAX;
    while i >= 0 {
        if remain == 0 {
            cache = rand::random::<u64>();
            remain = LETTER_IDX_MAX;
        }
        let idx: u32 = (cache & LETTER_IDX_MASK) as u32;
        if idx < letters.len() as u32 {
            b[i as usize] = letters[idx as usize];
            i -= 1;
        }
        cache >>= LETTER_IDX_BITS;
        remain -= 1;
    }

    b.iter().cloned().collect::<String>()
}
