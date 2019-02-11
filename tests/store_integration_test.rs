#[cfg(test)]
mod store_integration_test {
    use toy_kv::engine::{kv, store};

    use std::path::PathBuf;
    use tempfile::tempdir;

    fn tmpfile(name: &str) -> (PathBuf, PathBuf, PathBuf) {
        let tmp = tempdir().unwrap();
        let path = tmp.into_path();
        let mut key = path.clone();
        let mut value = path.clone();
        let mut buffer = path.clone();
        key.push(format!("{}.key", name));
        value.push(format!("{}.value", name));
        buffer.push(format!("{}.buffer", name));
        (key, value, buffer)
    }

    #[test]
    fn store_put_get() {
        let (k, v, b) = tmpfile("test_store_put_get");
        {
            let mut db = store::Store::new(&k, &v, &b).unwrap();
            let kvs = vec![
                ("key00", "value00"),
                ("key02", "value02"),
                ("key01", "value01"),
                ("key03", "value03"),
                ("key05", "value05"),
                ("key04", "value04"),
            ];
            for _ in 0..3 {
                for kv in &kvs {
                    db.put(
                        kv.0.parse().unwrap(),
                        kv::Value::Valid(Box::new(kv.1.parse().unwrap())),
                    )
                    .unwrap();
                }
            }
        }
        {
            // Restore from file
            let mut db = store::Store::new(&k, &v, &b).unwrap();

            for i in 0..=5 {
                let v = db
                    .get(format!("key0{}", i).parse().unwrap())
                    .unwrap()
                    .unwrap();
                assert_eq!(v.to_string(), format!("value0{}", i))
            }
        }
    }

    #[test]
    fn store_delete() {
        let (k, v, b) = tmpfile("test_store_delete");
        let mut db = store::Store::new(&k, &v, &b).unwrap();
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
                kv::Value::Valid(Box::new(kv.1.parse().unwrap())),
            )
            .unwrap();
        }
        db.delete(kvs[4].0.parse().unwrap()).unwrap();
        for i in 0..=4 {
            let v = db
                .get(format!("key0{}", i).parse().unwrap())
                .unwrap()
                .unwrap();
            assert_eq!(v.to_string(), format!("value0{}", i))
        }
        let invalid = db.get(kvs[4].0.parse().unwrap()).unwrap();
        assert!(invalid.is_none());
    }

    #[test]
    fn store_scan() {
        let (k, v, b) = tmpfile("test_store_scan");
        let mut db = store::Store::new(&k, &v, &b).unwrap();
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
                kv::Value::Valid(Box::new(kv.1.parse().unwrap())),
            )
            .unwrap();
        }
        let mut iter = db.scan(0, 6);
        for i in 0..=5 {
            let (k, v) = iter.next().unwrap();
            assert_eq!(k.to_string(), format!("key0{}", i));
            assert_eq!(v.to_string(), format!("value0{}", i));
        }
        let res = iter.next();
        assert!(res.is_none());
    }

    #[test]
    fn store_to_grow() {
        let (k, v, b) = tmpfile("test_store_to_grow");
        {
            let mut db = store::Store::new(&k, &v, &b).unwrap();
            for i in 0..100_000 {
                db.put(
                    format!("k{}", i).parse().unwrap(),
                    kv::Value::Valid(Box::new(format!("v{}", i).parse().unwrap())),
                )
                .unwrap();
            }
        }

        {
            // Restore from file
            let mut db = store::Store::new(&k, &v, &b).unwrap();
            // let v = db.get("k65536".parse().unwrap()).unwrap().unwrap();
            for i in 99_999..100_000 {
                let v = db.get(format!("k{}", i).parse().unwrap()).unwrap().unwrap();
                assert_eq!(v.to_string(), format!("v{}", i))
            }
            // assert_eq!(v.to_string(), "v65536");
        }
    }
}
