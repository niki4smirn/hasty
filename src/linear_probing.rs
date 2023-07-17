use crate::hash_table::HashTable;
use bincode::Options;
use serde::{Deserialize, Serialize};
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::io::prelude::*;
use std::os::unix::prelude::FileExt;
use std::{fs::OpenOptions, mem::size_of};

pub struct LPHashTable {
    file: std::fs::File,
    capacity: usize,
    len: usize,
    load_factor: f64,
    used_capacity: usize,
    block_size: usize,
}

pub struct LPHashTableOptions {
    pub filename: String,
}

#[derive(Serialize, Deserialize, Debug, Eq, PartialEq)]
pub struct LPHashTableEntry(pub Option<(u64, u64)>);

impl LPHashTableEntry {
    pub const fn bin_size() -> usize {
        size_of::<u64>() + size_of::<u64>() + 1
    }

    pub fn serialize(&self) -> bincode::Result<Vec<u8>> {
        let bincode_options = bincode::DefaultOptions::new()
            .with_fixint_encoding()
            .allow_trailing_bytes();
        bincode_options.serialize(&self).map(|mut v| {
            v.resize(Self::bin_size(), 0);
            v
        })
    }

    pub fn deserialize(bytes: &[u8]) -> bincode::Result<Self> {
        let bincode_options = bincode::DefaultOptions::new()
            .with_fixint_encoding()
            .allow_trailing_bytes();
        bincode_options.deserialize(bytes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
}

impl LPHashTable {
    pub fn new(options: &LPHashTableOptions) -> Self {
        let file_exists = std::path::Path::new(&options.filename).exists();
        if file_exists {
            println!("File already exists");
        }
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&options.filename)
            .unwrap();
        let mut capacity;
        let mut len = 0usize;
        if !file_exists {
            capacity = 1;
            while capacity * LPHashTableEntry::bin_size() < 2 * 1024 * 1024 {
                capacity *= 2;
            } // capacity is a power of 2
            for _ in 0..capacity {
                let entry = LPHashTableEntry(None);
                let bytes = entry.serialize().unwrap();
                file.write_all(&bytes).unwrap();
            }
        } else {
            debug_assert!(false);
            capacity = file.metadata().unwrap().len() as usize / LPHashTableEntry::bin_size();
            for pos in 0..capacity {
                if let Some(_) =
                    Self::read_pos(&file, (pos * LPHashTableEntry::bin_size()) as u64).0
                {
                    len += 1;
                }
            }
        }

        LPHashTable {
            file,
            capacity,
            len,
            load_factor: 0.5,
            used_capacity: capacity,
            block_size: capacity,
        }
    }

    fn hash(key: u64) -> u64 {
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        hasher.finish()
    }

    fn read_pos(file: &std::fs::File, pos: u64) -> LPHashTableEntry {
        let capacity = file.metadata().unwrap().len() as usize / LPHashTableEntry::bin_size();
        debug_assert!(pos < capacity as u64 * LPHashTableEntry::bin_size() as u64);
        debug_assert_eq!(pos % LPHashTableEntry::bin_size() as u64, 0);
        let mut bytes = [0; LPHashTableEntry::bin_size()];
        file.read_at(&mut bytes, pos).unwrap();
        LPHashTableEntry::deserialize(&bytes).unwrap()
    }

    fn key_to_pos(&self, key: u64) -> u64 {
        let cell_num = Self::hash(key) % self.capacity as u64;
        if cell_num < self.used_capacity as u64 {
            cell_num * LPHashTableEntry::bin_size() as u64
        } else {
            (cell_num - (self.capacity / 2) as u64) * LPHashTableEntry::bin_size() as u64
        }
    }

    fn read_key(&self, key: u64) -> (u64, LPHashTableEntry) {
        let mut pos = self.key_to_pos(key);
        let mut cur_entry;
        loop {
            cur_entry = Self::read_pos(&self.file, pos);
            match cur_entry {
                LPHashTableEntry(None) => {
                    break;
                }
                LPHashTableEntry(Some((cur_key, _))) => {
                    if cur_key == key {
                        break;
                    }
                }
            }
            pos += LPHashTableEntry::bin_size() as u64;
            if pos >= self.used_capacity as u64 * LPHashTableEntry::bin_size() as u64 {
                pos = 0;
            }
        }
        (pos, cur_entry)
    }

    fn resize_if_needed(&mut self) {
        if (self.len as f64 / self.used_capacity as f64) < self.load_factor {
            return;
        }
        if self.used_capacity == self.capacity {
            self.capacity *= 2;
        }

        for _ in 0..self.block_size {
            let empty_entry_bytes = LPHashTableEntry(None).serialize().unwrap();
            self.file.write_all(&empty_entry_bytes).unwrap();
        }

        let start = self.used_capacity - self.capacity / 2;

        self.used_capacity += self.block_size;
        for pos in start..(start + self.block_size) {
            let pos = pos * LPHashTableEntry::bin_size();
            if let Some((key, value)) = Self::read_pos(&self.file, pos as u64).0 {
                let new_pos = self.key_to_pos(key);
                if new_pos == pos as u64 {
                    continue;
                }

                let mut pos = pos;
                loop {
                    let cur_entry = Self::read_pos(&self.file, pos as u64);
                    match cur_entry {
                        LPHashTableEntry(None) => {
                            debug_assert!(false);
                        }
                        LPHashTableEntry(Some((cur_key, _))) => {
                            if cur_key == key {
                                break;
                            }
                        }
                    }
                    pos += LPHashTableEntry::bin_size();
                    if pos >= self.used_capacity * LPHashTableEntry::bin_size() {
                        pos = 0;
                    }
                }
                let empty_entry_bytes = LPHashTableEntry(None).serialize().unwrap();
                self.file
                    .write_all_at(&empty_entry_bytes, pos as u64)
                    .unwrap();
                self.len -= 1;
                self.set(key, value);
            }
        }
    }
}

impl HashTable for LPHashTable {
    fn set(&mut self, key: u64, value: u64) {
        let entry = LPHashTableEntry(Some((key, value)));
        let bytes = entry.serialize().unwrap();
        let (pos, pos_entry) = self.read_key(key);
        if pos_entry == LPHashTableEntry(None) {
            self.len += 1;
        }
        self.file.write_all_at(&bytes, pos).unwrap();

        self.resize_if_needed();
    }

    fn get(&self, key: u64) -> Option<u64> {
        let (_, entry) = self.read_key(key);
        match entry {
            LPHashTableEntry(None) => None,
            LPHashTableEntry(Some((_, value))) => Some(value),
        }
    }

    fn remove(&mut self, key: u64) {
        let (pos, pos_entry) = self.read_key(key);
        if pos_entry != LPHashTableEntry(None) {
            self.len -= 1;
            let entry = LPHashTableEntry(None);
            let bytes = entry.serialize().unwrap();
            self.file.write_all_at(&bytes, pos).unwrap();
        }
    }

    fn on_disk_size(&self) -> usize {
        self.file.metadata().unwrap().len() as usize
    }

    fn len(&self) -> usize {
        self.len
    }
}

impl Drop for LPHashTable {
    fn drop(&mut self) {
        self.file.sync_all().unwrap();
    }
}
