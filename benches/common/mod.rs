use rand::seq::SliceRandom;
use rand::thread_rng;
use std::fs;

use hasty::hash_table::HashTable;

pub fn read_input() -> Vec<(u64, u64)> {
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

pub fn measure<F: FnOnce()>(f: F) -> std::time::Duration {
    let start = std::time::Instant::now();
    f();
    start.elapsed()
}

pub fn run_write<T: HashTable>(table: &mut T) -> Vec<(std::time::Duration, usize)> {
    let input = read_input();
    let mut measurements = Vec::new();
    for (key, value) in input {
        measurements.push((measure(|| table.set(key, value)), table.on_disk_size()));
    }
    measurements
}

pub fn run_read_existing<T: HashTable>(
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

pub fn run_read_random<T: HashTable>(table: &mut T, reads_num: usize) -> Vec<std::time::Duration> {
    let mut durations = Vec::new();
    for _ in 0..reads_num {
        let key = rand::random::<u64>();
        durations.push(measure(|| {
            table.get(key);
        }));
    }
    durations
}
