use std::hash::{BuildHasher, Hash};
use std::collections::{BTreeSet, HashMap};
use std::collections::hash_map::RandomState;

// TODO clean everything up (maybe using hand crafted collections)

pub trait HasSize {
    fn size(&self) -> usize;
}

pub trait HasCost {
    fn cost(&self) -> usize;
}

struct StorageEntry<K, V>
    where V: HasSize + HasCost
{
    key: *const K,
    value: V,
    frequency: usize,
    clock: usize,
}

impl<K, V> StorageEntry<K, V>
    where V: HasSize + HasCost
{
    fn new(key: *const K, value: V, clock: usize) -> Self {
        StorageEntry {
            key: key,
            value: value,
            frequency: 1,
            clock: clock,
        }
    }
    fn priority(&self) -> usize {
        self.clock + self.frequency * self.value.cost() / self.value.size()
    }
    fn update(&mut self, clock: usize) -> usize {
        self.clock = clock;
        self.frequency += 1;
        self.priority()
    }
}

// Greedy Dual Size Frequency cache
pub struct GDSFCache<K, V, S = RandomState>
    where K: Eq + Hash,
          V: HasSize + HasCost,
          S: BuildHasher
{
    capacity: usize,
    used: usize,
    storage: HashMap<Box<K>, Box<StorageEntry<K, V>>, S>,
    queue: BTreeSet<(usize, *mut StorageEntry<K, V>)>,
    clock: usize,
}

impl<K, V, S> GDSFCache<K, V, S>
    where K: Eq + Hash,
          V: HasSize + HasCost,
          S: BuildHasher + Default
{
    pub fn new(capacity: usize) -> Self {
        GDSFCache {
            capacity: capacity,
            used: 0,
            storage: HashMap::default(),
            queue: BTreeSet::default(),
            clock: 0,
        }
    }
}

impl<K, V, S> GDSFCache<K, V, S>
    where K: Eq + Hash,
          V: HasSize + HasCost,
          S: BuildHasher
{
    pub fn contains_key(&self, key: &K) -> bool {
        self.storage.contains_key(key)
    }
    pub fn insert(&mut self, key: K, value: V) -> Result<&mut V, V> {
        self.remove(&key);

        let size = value.size();
        let key = Box::new(key);
        let key_ptr = key.as_ref() as *const _;
        let mut entry = Box::new(StorageEntry::new(key_ptr, value, self.clock));
        let entry_ptr = entry.as_mut() as *mut _;
        let priority = entry.priority();

        let excess = self.used + size - self.capacity;
        if excess > 0 {
            let pos = self.queue
                .iter()
                .take_while(|it| it.0 <= priority)
                .scan(0, |acc, it| {
                    *acc += unsafe { (*it.1).value.size() };
                    Some(*acc)
                })
                .position(|it| it >= excess);
            if let Some(num) = pos {
                for i in 0..(num + 1) {
                    let &(pri, entry_ptr) = self.queue.iter().next().unwrap();
                    // TODO get max priority from previous scan?
                    self.clock = pri;
                    unsafe {
                        self.remove(&*key);
                    };
                }
            } else {
                self.clock = priority;
            }

        };

        if self.used + size <= self.capacity {
            self.storage.insert(key, entry);
            self.queue.insert((priority, entry_ptr));
            self.used += size;
            unsafe { Ok(&mut (*entry_ptr).value) }
        } else {
            Err(entry.value)
        }
    }
    pub fn get_mut(&mut self, key: &K) -> Option<&mut V> {
        let queue = &mut self.queue;
        let clock = self.clock;
        self.storage.get_mut(key).map(|entry| {
            let entry_ptr = entry.as_mut() as *mut _;
            queue.remove(&(entry.priority(), entry_ptr));
            queue.insert((entry.update(clock), entry_ptr));
            &mut entry.value
        })
    }
    pub fn remove(&mut self, key: &K) -> Option<V> {
        self.storage.remove(key).map(|mut entry| {
            self.used -= entry.value.size();
            let entry_ptr = entry.as_mut() as *mut _;
            self.queue.remove(&(entry.priority(), entry_ptr));
            entry.value
        })
    }
}
