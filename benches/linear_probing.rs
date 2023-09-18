mod common;
use common::*;

use hasty::hash_table::HashTable;
use hasty::linear_probing::{LPHashTable, LPHashTableOptions};
use std::fs;
use std::io::Write;

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
