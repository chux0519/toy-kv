#[cfg(test)]
mod tests {
    use toy_kv::engine;

    #[test]
    fn store_put_get() {
        let mut db = engine::Store::new();
        let pairs = vec![
            ([0; 8], engine::Value::Value([0; 256])),
            ([2; 8], engine::Value::Value([2; 256])),
            ([1; 8], engine::Value::Value([1; 256])),
            ([3; 8], engine::Value::Value([3; 256])),
            ([5; 8], engine::Value::Value([5; 256])),
            ([4; 8], engine::Value::Value([4; 256])),
        ];
        for pair in pairs {
            db.put(pair.0, pair.1).unwrap();
        }
        for i in 0..=5 {
            let v = db.get(&[i; 8]).unwrap();
            assert!(v.iter().eq([i; 256].iter()));
        }
    }

    #[test]
    fn store_delete() {
        let mut db = engine::Store::new();
        let pairs = vec![
            ([0; 8], engine::Value::Value([0; 256])),
            ([2; 8], engine::Value::Value([2; 256])),
            ([1; 8], engine::Value::Value([1; 256])),
            ([3; 8], engine::Value::Value([3; 256])),
            ([5; 8], engine::Value::Value([5; 256])),
            ([4; 8], engine::Value::Value([4; 256])),
        ];
        for pair in pairs {
            db.put(pair.0, pair.1).unwrap();
        }
        db.delete([5; 8]).unwrap();
        for i in 0..=4 {
            let v = db.get(&[i; 8]).unwrap();
            assert!(v.iter().eq([i; 256].iter()));
        }
        let invalid = db.get(&[5; 8]);
        assert!(invalid.is_none());
    }

    #[test]
    fn store_scan() {
        let mut db = engine::Store::new();
        let pairs = vec![
            ([0; 8], engine::Value::Value([0; 256])),
            ([2; 8], engine::Value::Value([2; 256])),
            ([1; 8], engine::Value::Value([1; 256])),
            ([3; 8], engine::Value::Value([3; 256])),
            ([5; 8], engine::Value::Value([5; 256])),
            ([4; 8], engine::Value::Value([4; 256])),
        ];
        for pair in pairs {
            db.put(pair.0, pair.1).unwrap();
        }
        let mut iter = db.scan();
        for i in 0..=5 {
            let (k, v) = iter.next().unwrap();
            assert!(k.iter().eq([i; 8].iter()));
            assert!(v.iter().eq([i; 256].iter()));
        }
        let res = iter.next();
        assert!(res.is_none());
    }
}
// TODO: concurrent tests
