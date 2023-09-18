#[cfg(test)]
mod tests {
    use hasty::hash_table::HashTable;
    use hasty::linear_probing::{LPHashTable, LPHashTableEntry, LPHashTableOptions};
    use rand::Rng;
    use std::collections::HashMap;
    use std::fs;

    #[test]
    fn entry() {
        let test_entries = vec![
            LPHashTableEntry(None),
            LPHashTableEntry(Some((0, 0))),
            LPHashTableEntry(Some((1, 1))),
            LPHashTableEntry(Some((u64::MAX, u64::MAX))),
        ];
        for entry in test_entries {
            let bytes = entry.serialize().unwrap();
            assert_eq!(bytes.len(), LPHashTableEntry::bin_size());
            let entry2 = LPHashTableEntry::deserialize(&bytes).unwrap();
            assert_eq!(entry, entry2);
        }
    }

    #[test]
    fn check_correctness() {
        let filename = "lp1.bin".to_string();
        let mut my_table = LPHashTable::new(&LPHashTableOptions {
            filename: filename.clone(),
        });
        let mut table = HashMap::new();
        const ITERS: usize = 1e4 as usize;
        let mut rng = rand::thread_rng();
        for _ in 0..ITERS {
            let key = rng.gen::<u64>();
            let value = rng.gen::<u64>();
            my_table.set(key, value);
            table.insert(key, value);
        }
        for _ in 0..ITERS {
            let key = rng.gen::<u64>();
            assert_eq!(my_table.get(key), table.get(&key).copied());
        }
        fs::remove_file(filename).unwrap();
    }
}
