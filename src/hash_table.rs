pub trait HashTable {
    fn set(&mut self, key: u64, value: u64);
    fn get(&self, key: u64) -> Option<u64>;
    fn remove(&mut self, key: u64);
    fn len(&self) -> usize;
    fn on_disk_size(&self) -> usize;
}
