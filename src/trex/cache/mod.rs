pub mod ownable;
pub mod gdfs_cache;

use self::ownable::Ownable;
use self::gdfs_cache::{GDSFCache, HasCost, HasSize};
use std::borrow::Borrow;
use std::marker::PhantomData;
use std::hash::{BuildHasher, Hash, Hasher};
use std::collections::hash_map::{Entry, HashMap};
use std::sync::{Arc, Mutex};
use std::ops::Deref;
use lru_cache::LruCache;
use owning_ref::MutexGuardRefMut;

pub trait Cache {
    type K;
    type V;
    fn store(&mut self, k: Self::K, v: Self::V) -> Result<&mut Self::V, Self::V>;
    fn fetch(&mut self, k: &Self::K) -> Option<&mut Self::V>;
    fn contains(&mut self, k: &Self::K) -> bool;
    fn remove(&mut self, k: &Self::K) -> Option<Self::V>;
}

pub trait Fetcher<K, V> {
    fn fetch(&self, &K) -> V;
}

pub enum FetchedValue<'a, C: Cache + 'a> {
    Cached(MutexGuardRefMut<'a, C, C::V>),
    Uncached(C::V),
}

impl<'a, C: Cache + 'a> Deref for FetchedValue<'a, C> {
    type Target = C::V;
    fn deref(&self) -> &Self::Target {
        match *self {
            FetchedValue::Cached(ref val) => val,
            FetchedValue::Uncached(ref val) => val,
        }
    }
}

pub struct CachedFetcher<C, F>
    where C: Cache,
          F: Fetcher<C::K, C::V>
{
    // TODO make member variables private
    pub cache: Arc<Mutex<C>>,
    pub fetcher: F,
}

impl<C, F> CachedFetcher<C, F>
    where C: Cache + Default,
          F: Fetcher<C::K, C::V>
{
    pub fn new(fetcher: F) -> Self {
        CachedFetcher {
            cache: Arc::default(),
            fetcher: fetcher,
        }
    }
}

impl<C, F> CachedFetcher<C, F>
    where C: Cache,
          F: Fetcher<C::K, C::V>
{
    pub fn with_cache(cache: Arc<Mutex<C>>, fetcher: F) -> Self {
        CachedFetcher {
            cache: cache,
            fetcher: fetcher,
        }
    }
}

impl<C, F> CachedFetcher<C, F>
    where C: Cache,
          F: Fetcher<C::K, C::V>
{
    pub fn fetch<Q>(&self, key: Q) -> FetchedValue<C>
        where Q: Borrow<C::K> + Ownable<C::K>
    {
        let mut cache = MutexGuardRefMut::new(self.cache.lock().unwrap());
        if cache.contains(key.borrow()) {
            FetchedValue::Cached(cache.map(|cache| cache.fetch(key.borrow()).unwrap()))
        } else {
            drop(cache);
            let value = self.fetcher.fetch(key.borrow());
            MutexGuardRefMut::new(self.cache.lock().unwrap())
                .try_map(|cache| cache.store(key.into_owned(), value))
                .map(FetchedValue::Cached)
                .unwrap_or_else(FetchedValue::Uncached)
        }
    }
}

#[derive(Default, Clone, Debug)]
struct DummyCache<K, V>(PhantomData<K>, PhantomData<V>);

impl<K, V> Cache for DummyCache<K, V> {
    type K = K;
    type V = V;
    fn store(&mut self, k: Self::K, v: Self::V) -> Result<&mut Self::V, Self::V> {
        Err(v)
    }
    fn fetch(&mut self, k: &Self::K) -> Option<&mut Self::V> {
        None
    }
    fn contains(&mut self, k: &Self::K) -> bool {
        false
    }
    fn remove(&mut self, k: &Self::K) -> Option<Self::V> {
        None
    }
}

impl<K, V, S> Cache for HashMap<K, V, S>
    where K: Eq + Hash,
          S: BuildHasher
{
    type K = K;
    type V = V;
    fn store(&mut self, k: Self::K, v: Self::V) -> Result<&mut Self::V, Self::V> {
        match self.entry(k) {
            Entry::Occupied(mut entry) => {
                entry.insert(v);
                Ok(entry.into_mut())
            }
            Entry::Vacant(entry) => Ok(entry.insert(v)),
        }
    }
    fn fetch(&mut self, k: &Self::K) -> Option<&mut Self::V> {
        self.get_mut(k)
    }
    fn contains(&mut self, k: &Self::K) -> bool {
        self.contains_key(k)
    }
    fn remove(&mut self, k: &Self::K) -> Option<Self::V> {
        HashMap::remove(self, k)
    }
}

impl<K, V, S> Cache for LruCache<K, V, S>
    where K: Eq + Hash,
          S: BuildHasher
{
    type K = K;
    type V = V;
    fn store(&mut self, k: Self::K, v: Self::V) -> Result<&mut Self::V, Self::V> {
        self.insert(k, v);
        Ok(self.iter_mut().next_back().unwrap().1)
    }
    fn fetch(&mut self, k: &Self::K) -> Option<&mut Self::V> {
        self.get_mut(k)
    }
    fn contains(&mut self, k: &Self::K) -> bool {
        self.contains_key(k)
    }
    fn remove(&mut self, k: &Self::K) -> Option<Self::V> {
        LruCache::remove(self, k)
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

impl<K, V, S> Cache for GDSFCache<K, V, S>
    where K: Eq + Hash,
          V: HasSize + HasCost,
          S: BuildHasher
{
    type K = K;
    type V = V;
    fn store(&mut self, k: Self::K, v: Self::V) -> Result<&mut Self::V, Self::V> {
        self.insert(k, v)
    }
    fn fetch(&mut self, k: &Self::K) -> Option<&mut Self::V> {
        self.get_mut(k)
    }
    fn contains(&mut self, k: &Self::K) -> bool {
        self.contains_key(k)
    }
    fn remove(&mut self, k: &Self::K) -> Option<Self::V> {
        GDSFCache::remove(self, k)
    }
}
