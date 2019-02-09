use toy_kv::engine::kv;
use toy_kv::transport::open_db_from;

mod util;

use tempfile::tempdir;
use time::PreciseTime;
use util::gen_rand_string;

fn main() {
    let tmp = tempdir().unwrap();
    let mut db = open_db_from(&tmp.into_path()).unwrap();
    let start = PreciseTime::now();
    for _ in 0..100000 {
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
        (100000 * (256 + 8) / 1024 / 1024) as f64
            / (start.to(end).num_nanoseconds().unwrap() as f64 / 1e9) as f64
    );
}
