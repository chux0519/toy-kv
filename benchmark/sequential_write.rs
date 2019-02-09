use toy_kv::engine::kv;
use toy_kv::transport::open_db_from;

use tempfile::tempdir;
use time::PreciseTime;

fn main() {
    let tmp = tempdir().unwrap();
    let mut db = open_db_from(&tmp.into_path()).unwrap();
    let start = PreciseTime::now();
    for i in 0..100_000 {
        db.put(
            format!("k{}", i).parse().unwrap(),
            kv::Value::Valid(Box::new(format!("v{}", i).parse().unwrap())),
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
    for i in 0..100_000 {
        let v = db.get(format!("k{}", i).parse().unwrap()).unwrap().unwrap();
        assert_eq!(v.to_string(), format!("v{}", i));
    }
}
