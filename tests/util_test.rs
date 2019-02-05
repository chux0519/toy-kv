#[cfg(test)]
mod util_tests {

    #[cfg(test)]
    mod build_index_tests {
        use toy_kv::engine::util;
        #[test]
        fn broken_test() {
            let data = [0; 11];
            let index = util::build_index(&data);
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
            let index = util::build_index(&data).unwrap();
            // ventry should be ordered as: 1, 2, 0, 3
            let entries: Vec<usize> = index.iter().map(|key| key.ventry).collect();
            dbg!(&entries);
            assert_eq!(entries, [1, 2, 0, 3]);
        }
    }
}
