use lru_cache::LruCache;
use std::hash::{BuildHasher, Hash, Hasher};
use std::collections::{BTreeSet, HashMap};
use std::collections::hash_map::RandomState;
use std::borrow::{Borrow, Cow};

pub trait HasSize {
    fn size(&self) -> usize;
}

pub trait HasCost {
    fn cost(&self) -> usize;
}

pub enum FetchedValue<'a, T: 'a> {
    Cached(&'a mut T),
    Uncached(T),
}

pub trait Fetcher<K: Clone, V> {
    fn fetch(&mut self, key: Cow<K>) -> FetchedValue<V>;
}

pub trait Cache<K, V>
    where K: Eq + Hash
{
    fn store(&mut self, k: K, v: V) -> Option<V>;
    fn fetch<Q: ?Sized>(&mut self, k: &Q) -> Option<&mut V>
        where K: Borrow<Q>,
              Q: Hash + Eq;
}

impl<K, V, S> Cache<K, V> for HashMap<K, V, S>
    where K: Eq + Hash,
          S: BuildHasher
{
    fn store(&mut self, k: K, v: V) -> Option<V> {
        self.insert(k, v)
    }
    fn fetch<Q: ?Sized>(&mut self, k: &Q) -> Option<&mut V>
        where K: Borrow<Q>,
              Q: Hash + Eq
    {
        self.get_mut(k)
    }
}

impl<K, V, S> Cache<K, V> for LruCache<K, V, S>
    where K: Eq + Hash,
          S: BuildHasher
{
    fn store(&mut self, k: K, v: V) -> Option<V> {
        self.insert(k, v)
    }
    fn fetch<Q: ?Sized>(&mut self, k: &Q) -> Option<&mut V>
        where K: Borrow<Q>,
              Q: Hash + Eq
    {
        self.get_mut(k)
    }
}

struct ModHasher<H: Hasher> {
    hasher: H,
    modulus: u64,
}

impl<H: Hasher> Hasher for ModHasher<H> {
    fn finish(&self) -> u64 {
        self.hasher.finish() % self.modulus
    }
    fn write(&mut self, bytes: &[u8]) {
        self.hasher.write(bytes)
    }

    fn write_u8(&mut self, i: u8) {
        self.hasher.write_u8(i)
    }
    fn write_u16(&mut self, i: u16) {
        self.hasher.write_u16(i)
    }
    fn write_u32(&mut self, i: u32) {
        self.hasher.write_u32(i)
    }
    fn write_u64(&mut self, i: u64) {
        self.hasher.write_u64(i)
    }
    fn write_usize(&mut self, i: usize) {
        self.hasher.write_usize(i)
    }
    fn write_i8(&mut self, i: i8) {
        self.hasher.write_i8(i)
    }
    fn write_i16(&mut self, i: i16) {
        self.hasher.write_i16(i)
    }
    fn write_i32(&mut self, i: i32) {
        self.hasher.write_i32(i)
    }
    fn write_i64(&mut self, i: i64) {
        self.hasher.write_i64(i)
    }
    fn write_isize(&mut self, i: isize) {
        self.hasher.write_isize(i)
    }
}

struct ModBuildHasher<S: BuildHasher> {
    hash_builder: S,
    modulus: u64,
}

impl<S: BuildHasher + Default> ModBuildHasher<S> {
    fn new(modulus: u64) -> Self {
        ModBuildHasher {
            hash_builder: S::default(),
            modulus: modulus,
        }
    }
}

impl<S: BuildHasher> ModBuildHasher<S> {
    fn with_modulus_and_hasher(modulus: u64, hash_builder: S) -> Self {
        ModBuildHasher {
            hash_builder: hash_builder,
            modulus: modulus,
        }
    }
}

impl<S: BuildHasher> BuildHasher for ModBuildHasher<S> {
    type Hasher = ModHasher<S::Hasher>;
    fn build_hasher(&self) -> Self::Hasher {
        ModHasher {
            hasher: self.hash_builder.build_hasher(),
            modulus: self.modulus,
        }
    }
}

type CollisionCache<K, V, S> = HashMap<K, V, ModBuildHasher<S>>;

struct StorageEntry<K, V>
    where V: HasSize + HasCost
{
    key: *const K,
    value: Box<V>,
    frequency: usize,
    clock: usize,
}

impl<K, V> StorageEntry<K, V>
    where V: HasSize + HasCost
{
    fn new(key: *const K, value: Box<V>, clock: usize) -> Self {
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
struct GDSFCache<K, V, S = RandomState>
    where K: Eq + Hash,
          V: HasSize + HasCost,
          S: BuildHasher
{
    capacity: usize,
    used: usize,
    storage: HashMap<Box<K>, StorageEntry<K, V>, S>,
    queue: BTreeSet<(usize, *const K, usize)>,
    clock: usize,
}

impl<K, V, S> GDSFCache<K, V, S>
    where K: Eq + Hash,
          V: HasSize + HasCost,
          S: BuildHasher + Default
{
    fn new(capacity: usize) -> Self {
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
    fn contains_key(&self, key: &K) -> bool {
        self.storage.contains_key(key)
    }
    fn insert(&mut self, key: K, value: V) -> Result<&mut V, V> {
        // possibly remove previous entry with same key
        // if size + used <= capacity, then just insert
        // else check queue elems until sum sizes >= (size + used - capacity)
        // if all priorities < insert priority remove all and insert new
        // else discard new and keep the rest
        self.remove(&key);

        let size = value.size();
        let key = Box::new(key);
        let key_ptr = key.as_ref() as *const K;
        let mut value = Box::new(value);
        let value_ptr = value.as_mut() as *mut V;
        let storage_entry = StorageEntry::new(key_ptr, value, self.clock);
        let priority = storage_entry.priority();

        let excess = self.used + size - self.capacity;
        if excess > 0 {
            let pos = self.queue
                .iter()
                .take_while(|it| it.0 <= priority)
                .scan(0, |acc, it| {
                    *acc += it.2;
                    Some(*acc)
                })
                .position(|it| it >= excess);
            if let Some(num) = pos {
                for i in 0..(num + 1) {
                    let &(pri, key, _) = self.queue.iter().next().unwrap();
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
            self.storage.insert(key, storage_entry);
            self.queue.insert((priority, key_ptr, size));
            self.used += size;
            unsafe { Ok(&mut *value_ptr) }
        } else {
            Err(*storage_entry.value)
        }
    }
    fn get_mut(&mut self, key: &K) -> Option<&mut V> {
        let queue = &mut self.queue;
        let clock = self.clock;
        self.storage.get_mut(key).map(|storage_entry| {
            queue.remove(&(storage_entry.priority(), storage_entry.key, storage_entry.value.size()));
            queue.insert((storage_entry.update(clock), storage_entry.key, storage_entry.value.size()));
            &mut *storage_entry.value
        })
    }
    fn remove(&mut self, key: &K) -> Option<V> {
        self.storage.remove(key).map(|entry| {
            self.used -= entry.value.size();
            self.queue.remove(&(entry.priority(), entry.key, entry.value.size()));
            *entry.value
        })
    }
}

struct GDSFFetcher<K, V, S, F>
    where K: Eq + Hash + Clone,
          V: HasSize + HasCost,
          S: BuildHasher,
          F: Fn(&K) -> V
{
    cache: GDSFCache<K, V, S>,
    fetch: F,
}

impl<K, V, S, F> GDSFFetcher<K, V, S, F>
    where K: Eq + Hash + Clone,
          V: HasSize + HasCost,
          S: BuildHasher + Default,
          F: Fn(&K) -> V
{
    fn new(capacity: usize, fetch: F) -> Self {
        GDSFFetcher {
            cache: GDSFCache::new(capacity),
            fetch: fetch,
        }
    }
}

impl<K, V, S, F> Fetcher<K, V> for GDSFFetcher<K, V, S, F>
    where K: Eq + Hash + Clone,
          V: HasSize + HasCost,
          S: BuildHasher,
          F: Fn(&K) -> V
{
    fn fetch(&mut self, key: Cow<K>) -> FetchedValue<V> {
        if self.cache.contains_key(key.as_ref()) {
            FetchedValue::Cached(self.cache.get_mut(key.as_ref()).unwrap())
        } else {
            let value = (self.fetch)(key.as_ref());
            match self.cache.insert(key.into_owned(), value) {
                Ok(val) => FetchedValue::Cached(val),
                Err(val) => FetchedValue::Uncached(val),
            }
        }
    }
}
