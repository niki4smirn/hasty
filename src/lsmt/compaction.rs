use crate::lsmt::{Disktable, DISKTABLE_REPOSITORY};

pub(crate) trait Compactor {
    fn add(&mut self, disktable: Disktable);
    fn get(&self, key: u64) -> Option<u64>;
    fn on_disk_size(&self) -> usize;
}

pub(crate) struct TieredCompaction {
    max_files_per_level: usize,
    levels: Vec<Vec<Disktable>>,
}

impl TieredCompaction {
    pub fn new(max_files_per_level: usize) -> Self {
        Self {
            max_files_per_level,
            levels: Vec::new(),
        }
    }
}

impl Compactor for TieredCompaction {
    fn add(&mut self, disktable: Disktable) {
        if self.levels.is_empty() {
            self.levels.push(Vec::new());
        }
        self.levels[0].push(disktable);
        if self.levels[0].len() > self.max_files_per_level {
            let mut level = 0;
            while self.levels[level].len() > self.max_files_per_level {
                let mut temp = Vec::new();
                std::mem::swap(&mut self.levels[level], &mut temp);
                if level + 1 == self.levels.len() {
                    self.levels.push(Vec::new());
                }
                self.levels[level + 1].push(DISKTABLE_REPOSITORY.lock().unwrap().merge(temp));
                level += 1;
            }
        }
    }

    fn get(&self, key: u64) -> Option<u64> {
        let mut found_rev = None;
        let mut found_val = None;
        for level in self.levels.iter() {
            for disktable in level.iter().rev() {
                if let Some((val, rev)) = disktable.get(key) {
                    if found_rev.is_none() || found_rev.unwrap() < rev {
                        found_rev = Some(rev);
                        found_val = Some(val);
                    }
                }
            }
        }
        found_val
    }

    fn on_disk_size(&self) -> usize {
        self.levels
            .iter()
            .flatten()
            .map(|disktable| disktable.on_disk_size())
            .sum()
    }
}