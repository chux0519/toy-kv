#[cfg(test)]
mod store_integration_test {
    use std::path::PathBuf;
    use tempfile::tempdir;
    use toy_kv::engine::{kv, store};

    fn tmpfile(name: &str) -> PathBuf {
        let tmp = tempdir().unwrap();
        let mut path = tmp.into_path();
        path.push(name);
        path
    }
    #[test]
    fn store_put_get() {
        let tmpfile = tmpfile("test");
        let mut db = store::Store::new(tmpfile);
        let kvs = vec![
            ("key00", "value00"),
            ("key02", "value02"),
            ("key01", "value01"),
            ("key03", "value03"),
            ("key05", "value05"),
            ("key04", "value04"),
        ];
        for kv in kvs {
            db.put(
                kv.0.parse().unwrap(),
                kv::Value::Valid(kv.1.parse().unwrap()),
            )
            .unwrap();
        }
        for i in 0..=5 {
            let v = db.get(format!("key0{}", i).parse().unwrap()).unwrap();
            assert_eq!(v.to_string(), format!("value0{}", i))
        }
    }

    #[test]
    fn store_delete() {
        let tmpfile = tmpfile("test");
        let mut db = store::Store::new(tmpfile);
        let kvs = vec![
            ("key00", "value00"),
            ("key02", "value02"),
            ("key01", "value01"),
            ("key03", "value03"),
            ("key05", "value05"),
            ("key04", "value04"),
        ];
        for kv in &kvs {
            db.put(
                kv.0.parse().unwrap(),
                kv::Value::Valid(kv.1.parse().unwrap()),
            )
            .unwrap();
        }
        db.delete(kvs[4].0.parse().unwrap()).unwrap();
        for i in 0..=4 {
            let v = db.get(format!("key0{}", i).parse().unwrap()).unwrap();
            assert_eq!(v.to_string(), format!("value0{}", i))
        }
        let invalid = db.get(kvs[4].0.parse().unwrap());
        assert!(invalid.is_none());
    }

    #[test]
    fn store_scan() {
        let tmpfile = tmpfile("test");
        let mut db = store::Store::new(tmpfile);
        let kvs = vec![
            ("key00", "value00"),
            ("key02", "value02"),
            ("key01", "value01"),
            ("key03", "value03"),
            ("key05", "value05"),
            ("key04", "value04"),
        ];
        for kv in kvs {
            db.put(
                kv.0.parse().unwrap(),
                kv::Value::Valid(kv.1.parse().unwrap()),
            )
            .unwrap();
        }
        let mut iter = db.scan();
        for i in 0..=5 {
            let (k, v) = iter.next().unwrap();
            assert_eq!(k.to_string(), format!("key0{}", i));
            assert_eq!(v.to_string(), format!("value0{}", i));
        }
        let res = iter.next();
        assert!(res.is_none());
    }
}
// TODO: concurrent tests
