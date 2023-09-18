#[cfg(test)]
mod tests {
    use hasty::lsmt::LSMTree;
    use rand::Rng;
    use std::collections::HashMap;
    use hasty::hash_table::HashTable;

    #[test]
    fn check_correctness() {
        let mut my_table = LSMTree::new(1e3 as usize);
        let mut table = HashMap::new();
        const WRITE_ITERS: usize = 1e4 as usize;
        let mut rng = rand::thread_rng();
        for _ in 0..WRITE_ITERS {
            let key = rng.gen::<u64>();
            let value = rng.gen::<u64>();
            my_table.set(key, value);
            table.insert(key, value);
        }
        const READ_ITERS: usize = 1e3 as usize;
        for i in 0..READ_ITERS {
            let key = rng.gen::<u64>();
            if i % 100 == 0 {
                println!("{}/{}", i, READ_ITERS);
            }
            assert_eq!(my_table.get(key), table.get(&key).copied());
        }
    }
}
