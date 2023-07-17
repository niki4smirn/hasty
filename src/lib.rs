mod hash_table;
mod linear_probing;

#[cfg(test)]
mod tests {
    use super::*;
    use rand::seq::SliceRandom;
    use rand::thread_rng;
    use std::fs;
    use std::io::Write;

    use hash_table::HashTable;

    fn read_input() -> Vec<(u64, u64)> {
        let content = fs::read_to_string("input.txt").unwrap();
        content
            .lines()
            .map(|line| (line.split_whitespace().collect::<Vec<&str>>()))
            .map(|strings| {
                strings
                    .iter()
                    .map(|s| s.parse::<u64>().unwrap())
                    .collect::<Vec<u64>>()
            })
            .map(|numbers| (numbers[0], numbers[1]))
            .collect()
    }

    fn measure<F: FnOnce()>(f: F) -> std::time::Duration {
        let start = std::time::Instant::now();
        f();
        start.elapsed()
    }

    fn run_write<T: HashTable>(table: &mut T) -> Vec<(std::time::Duration, usize)> {
        let input = read_input();
        let mut measurements = Vec::new();
        for (key, value) in input {
            measurements.push((measure(|| table.set(key, value)), table.on_disk_size()));
        }
        measurements
    }

    fn run_read_existing<T: HashTable>(
        table: &mut T,
        mut present_elements: Vec<u64>,
        reads_num: usize,
    ) -> Vec<std::time::Duration> {
        let mut durations = Vec::new();
        present_elements.shuffle(&mut thread_rng());
        for _ in 0..reads_num {
            let pos = rand::random::<usize>() % present_elements.len();
            let key = present_elements[pos];
            durations.push(measure(|| {
                table.get(key);
            }));
        }
        durations
    }

    fn run_read_random<T: HashTable>(table: &mut T, reads_num: usize) -> Vec<std::time::Duration> {
        let mut durations = Vec::new();
        for _ in 0..reads_num {
            let key = rand::random::<u64>();
            durations.push(measure(|| {
                table.get(key);
            }));
        }
        durations
    }

    use linear_probing::{LPHashTable, LPHashTableOptions};
    use rand::Rng;
    use std::collections::HashMap;

    #[test]
    fn measure_write() {
        let filename = "lp4.bin".to_string();
        let mut table = LPHashTable::new(&LPHashTableOptions {
            filename: filename.clone(),
        });
        let measurements = run_write(&mut table);
        let mut file = fs::File::create("lp_write.txt").unwrap();
        for (duration, size) in measurements {
            writeln!(file, "{} {}", duration.as_nanos(), size).unwrap();
        }
        fs::remove_file(filename).unwrap();
    }

    use std::collections::HashSet;

    #[test]
    fn measure_read_existing() {
        let filename = "lp3.bin".to_string();
        const READS_NUM: usize = 1e7 as usize;
        let mut table = LPHashTable::new(&LPHashTableOptions {
            filename: filename.clone(),
        });
        let input = read_input();
        let mut read_pos = 0;
        let sizes = vec![100, 1000, 10000, 100000, 1000000];
        let mut present_elements_set = HashSet::new();
        for size in sizes {
            while present_elements_set.len() < size {
                let (key, value) = input[read_pos];
                if !present_elements_set.contains(&key) {
                    present_elements_set.insert(key);
                }
                table.set(key, value);
                read_pos += 1;
            }
            let present_elements_vec = present_elements_set.iter().copied().collect::<Vec<u64>>();
            let durations = run_read_existing(&mut table, present_elements_vec, READS_NUM);
            let mut file = fs::File::create(format!("lp_read_existing_{}.txt", size)).unwrap();
            for duration in durations {
                writeln!(file, "{}", duration.as_nanos()).unwrap();
            }
        }
        fs::remove_file(filename).unwrap();
    }

    #[test]
    fn measure_read_random() {
        let filename = "lp2.bin".to_string();
        const READS_NUM: usize = 1e7 as usize;
        let mut table = LPHashTable::new(&LPHashTableOptions {
            filename: filename.clone(),
        });
        let input = read_input();
        let mut read_pos = 0;
        let sizes = vec![100, 1000, 10000, 100000, 1000000];
        for size in sizes {
            while read_pos < size {
                let (key, value) = input[read_pos];
                table.set(key, value);
                read_pos += 1;
            }
            let durations = run_read_random(&mut table, READS_NUM);
            let mut file = fs::File::create(format!("lp_read_random_{}.txt", size)).unwrap();
            for duration in durations {
                writeln!(file, "{}", duration.as_nanos()).unwrap();
            }
        }
        fs::remove_file(filename).unwrap();
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
            assert_eq!(my_table.len(), table.len());
        }
        for _ in 0..ITERS {
            let key = rng.gen::<u64>();
            assert_eq!(my_table.get(key), table.get(&key).copied());
        }
        fs::remove_file(filename).unwrap();
    }
}
