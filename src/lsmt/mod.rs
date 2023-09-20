mod compaction;

use crate::hash_table::HashTable;
use bincode::{DefaultOptions, Options};
use once_cell::sync::Lazy;
use rand::{distributions::Alphanumeric, Rng};
use serde::{Deserialize, Serialize};
use std::{
    cmp::Reverse,
    collections::HashMap,
    fs::{self, OpenOptions},
    io::Write,
    os::unix::prelude::{FileExt, MetadataExt},
    sync::Mutex,
};

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
enum DisktableEntry {
    Insert { rev: u64, key: u64, value: u64 },
    Delete { rev: u64, key: u64 },
}

impl DisktableEntry {
    fn get_key(&self) -> u64 {
        match self {
            DisktableEntry::Insert {
                rev: _,
                key,
                value: _,
            } => *key,
            DisktableEntry::Delete { rev: _, key } => *key,
        }
    }

    fn get_rev(&self) -> u64 {
        match self {
            DisktableEntry::Insert {
                rev,
                key: _,
                value: _,
            } => *rev,
            DisktableEntry::Delete { rev, key: _ } => *rev,
        }
    }

    fn serialize(&self) -> bincode::Result<Vec<u8>> {
        let opts = DefaultOptions::new().allow_trailing_bytes();
        opts.serialize(&self).map(|mut v| {
            v.resize(Self::bin_size(), 0);
            v
        })
    }

    fn deserialize(bytes: &[u8]) -> bincode::Result<Self> {
        let opts = DefaultOptions::new().allow_trailing_bytes();
        opts.deserialize(bytes)
    }

    // WARNING
    const fn bin_size() -> usize {
        28 as usize
    }
}

pub(crate) struct Disktable {
    file: fs::File,
    size: usize,
}

struct DisktableIter<'a> {
    disktable: &'a Disktable,
    pos: usize,
}

impl<'a> Iterator for DisktableIter<'a> {
    type Item = DisktableEntry;

    fn next(&mut self) -> Option<Self::Item> {
        if self.pos == self.disktable.len() {
            return None;
        }
        let res = self.disktable.read_pos(self.pos);
        self.pos += 1;
        Some(res)
    }
}

struct DisktableRepository {
    used_filenames: Vec<String>,
    // each inode correspnds used_filename
    inodes: Vec<u64>,
    last_rev: u64,
}

impl DisktableRepository {
    fn new() -> Self {
        Self {
            used_filenames: Vec::new(),
            inodes: Vec::new(),
            last_rev: 0,
        }
    }

    const FILENAME_LEN: usize = 12;
    fn generate_filename(&mut self) -> String {
        rand::thread_rng()
            .sample_iter(&Alphanumeric)
            .take(Self::FILENAME_LEN)
            .map(char::from)
            .collect()
    }

    fn create_file(&mut self) -> fs::File {
        let mut filename = self.generate_filename();
        while self.used_filenames.contains(&filename) {
            filename = self.generate_filename();
        }

        self.used_filenames.push(filename.clone());

        fs::create_dir_all("lsmt").unwrap();

        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(format!("lsmt/{}", filename))
            .unwrap();

        self.inodes.push(file.metadata().unwrap().ino());

        file
    }

    fn delete_file(&mut self, inode: u64) {
        let mut index = None;
        for i in 0..self.inodes.len() {
            if self.inodes[i] == inode {
                index = Some(i);
                break;
            }
        }
        assert!(index.is_some());
        let index = index.unwrap();
        let filename = self.used_filenames.remove(index);
        fs::remove_file(format!("lsmt/{}", filename)).unwrap();
        self.inodes.remove(index);
    }

    fn from_memtable(&mut self, memtable: Memtable) -> Disktable {
        self.last_rev += 1;
        let rev = self.last_rev;
        let mut entries = memtable
            .into_iter()
            .map(|(k, v)| {
                if let Some(val) = v {
                    DisktableEntry::Insert {
                        rev,
                        key: k,
                        value: val,
                    }
                } else {
                    DisktableEntry::Delete { rev, key: k }
                }
            })
            .collect::<Vec<DisktableEntry>>();
        entries.sort_unstable_by(|entry1, entry2| entry1.get_key().cmp(&entry2.get_key()));
        self.from_iter(entries.into_iter())
    }

    fn from_iter<T: IntoIterator<Item = DisktableEntry>>(&mut self, iter: T) -> Disktable {
        let mut size = 0;
        let mut file = self.create_file();
        for entry in iter {
            let bytes = entry.serialize().unwrap();
            file.write_all(&bytes).unwrap();
            size += 1;
        }

        Disktable { file, size }
    }

    fn merge(&mut self, disktables: Vec<Disktable>) -> Disktable {
        let mut merged = Vec::with_capacity(disktables.iter().map(|dtable| dtable.len()).sum());
        let mut heap = std::collections::BinaryHeap::from_iter((0..disktables.len()).map(|i| {
            let entry = &disktables[i].read_pos(0);
            (Reverse(entry.get_key()), entry.get_rev(), 0, i)
        }));
        let mut last_key = None;
        while let Some((key, _, pos, i)) = heap.pop() {
            if last_key == Some(key) {
                continue;
            }
            last_key = Some(key);

            merged.push(disktables[i].read_pos(pos));
            if pos + 1 < disktables[i].len() {
                let next_entry = &disktables[i].read_pos(pos + 1);
                heap.push((
                    Reverse(next_entry.get_key()),
                    next_entry.get_rev(),
                    pos + 1,
                    i,
                ));
            }
        }

        for disktable in disktables {
            self.delete_file(disktable.file.metadata().unwrap().ino());
        }

        self.from_iter(merged.into_iter())
    }
}

static DISKTABLE_REPOSITORY: Lazy<Mutex<DisktableRepository>> =
    Lazy::new(|| Mutex::new(DisktableRepository::new()));

impl<'a> Disktable {
    fn iter(&'a self) -> DisktableIter<'a> {
        DisktableIter {
            disktable: self,
            pos: 0,
        }
    }
}

impl Disktable {
    // returns (value, rev)
    fn get(&self, key: u64) -> Option<(u64, u64)> {
        for read in self.iter() {
            if read.get_key() != key {
                continue;
            }
            match read {
                DisktableEntry::Insert { rev, key: _, value } => return Some((value, rev)),
                DisktableEntry::Delete { rev: _, key: _ } => return None,
            }
        }
        None
    }

    fn read_pos(&self, pos: usize) -> DisktableEntry {
        assert!(pos < self.len());
        let mut bytes = [0; DisktableEntry::bin_size()];
        self.file
            .read_at(&mut bytes, (pos * DisktableEntry::bin_size()) as u64)
            .unwrap();
        DisktableEntry::deserialize(&bytes).unwrap()
    }

    fn on_disk_size(&self) -> usize {
        self.file.metadata().unwrap().len() as usize
    }

    fn len(&self) -> usize {
        self.size
    }
}

type Memtable = HashMap<u64, Option<u64>>;

impl From<Memtable> for Disktable {
    fn from(memtable: Memtable) -> Self {
        DISKTABLE_REPOSITORY.lock().unwrap().from_memtable(memtable)
    }
}

impl FromIterator<DisktableEntry> for Disktable {
    fn from_iter<T: IntoIterator<Item = DisktableEntry>>(iter: T) -> Self {
        DISKTABLE_REPOSITORY.lock().unwrap().from_iter(iter)
    }
}

pub struct LSMTree {
    memtable: Memtable,
    compactor: Box<dyn compaction::Compactor>,
    mem_sz_threshold: usize,
    disktable_num: usize,
}

impl LSMTree {
    pub fn new(memtable_capacity: usize, max_tier_size: usize) -> Self {
        LSMTree {
            memtable: Memtable::new(),
            compactor: Box::new(compaction::TieredCompaction::new(max_tier_size)),
            mem_sz_threshold: memtable_capacity,
            disktable_num: 0,
        }
    }
}

impl HashTable for LSMTree {
    fn set(&mut self, key: u64, value: u64) {
        self.memtable.insert(key, Some(value));
        self.flush_on_threshold();
    }

    fn get(&self, key: u64) -> Option<u64> {
        if let Some(value) = self.memtable.get(&key) {
            return *value;
        }
        self.compactor.get(key)
    }

    fn on_disk_size(&self) -> usize {
        self.compactor.on_disk_size()
    }

    fn remove(&mut self, key: u64) {
        self.memtable.insert(key, None);
        self.flush_on_threshold();
    }
}

impl LSMTree {
    fn flush_on_threshold(&mut self) {
        if self.memtable.len() >= self.mem_sz_threshold {
            let disktable = Disktable::from(self.memtable.clone());
            self.compactor.add(disktable);
            self.memtable.clear();
            self.disktable_num += 1;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn entry_serde() {
        {
            let opts = DefaultOptions::new().allow_trailing_bytes();
            let val = u64::max_value();
            let serialized = opts
                .serialize(&DisktableEntry::Insert {
                    rev: val,
                    key: val,
                    value: val,
                })
                .unwrap();
            assert_eq!(DisktableEntry::bin_size(), serialized.len());
        }
        let tests = [
            DisktableEntry::Insert {
                rev: 0,
                key: 124,
                value: 421,
            },
            DisktableEntry::Insert {
                rev: 2,
                key: 0,
                value: 9,
            },
            DisktableEntry::Insert {
                rev: 1,
                key: 1,
                value: 21,
            },
            DisktableEntry::Delete { rev: 123, key: 9 },
            DisktableEntry::Delete { rev: 13, key: 91 },
        ];

        for test in tests {
            let serialized = test.serialize().unwrap();
            let deserialized = DisktableEntry::deserialize(serialized.as_ref()).unwrap();
            assert_eq!(test, deserialized);
        }
    }
}
