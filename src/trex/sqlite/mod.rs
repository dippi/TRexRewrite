mod query_builder;

use tesla::*;
use tesla::expressions::*;
use tesla::predicates::*;
use trex::NodeProvider;
use trex::rule_processor::*;
use trex::expressions::evaluation::*;
use trex::cache::{Cache, CachedFetcher, CollisionCache, DummyCache, Fetcher, ModBuildHasher};
use trex::cache::gdfs_cache::{GDSFCache, HasCost, HasSize};
use self::query_builder::SqlContext;
use linear_map::LinearMap;
use lru_cache::LruCache;
use rusqlite::Row;
use rusqlite::types::{ToSql, Value as SqlValue};
use r2d2::{Config, Pool};
use r2d2_sqlite::SqliteConnectionManager;
use chrono::UTC;
use std::sync::{Arc, Mutex};
use std::iter;
use std::usize;

#[derive(Debug, Hash, PartialEq, Eq)]
pub struct CacheKey {
    statement: String,
    input_params: Vec<Value>,
}

#[derive(Debug)]
pub enum CacheEntryValue {
    Values(usize, Vec<Value>),
    Aggr(Value),
    Count(usize),
    Exists(bool),
}

#[derive(Debug)]
pub struct CacheEntry {
    cost: usize,
    value: CacheEntryValue,
}

impl HasCost for CacheEntry {
    fn cost(&self) -> usize {
        self.cost
    }
}

impl HasSize for CacheEntry {
    fn size(&self) -> usize {
        match self.value {
            CacheEntryValue::Values(_, ref val) => val.len(),
            CacheEntryValue::Aggr(..) => 1,
            CacheEntryValue::Count(val) => val,
            CacheEntryValue::Exists(..) => 1,
        }
    }
}

pub trait SqlCache: Cache<K = CacheKey, V = CacheEntry> + Send {}
impl<T: Cache<K = CacheKey, V = CacheEntry> + Send + ?Sized> SqlCache for T {}

struct SqlFetcher {
    predicate: Predicate,
    statement: String,
    input_params: Vec<(usize, usize)>,
    output_params: Vec<BasicType>,
    pool: Pool<SqliteConnectionManager>,
}

impl SqlFetcher {
    fn prepare_key(&self, result: &PartialResult) -> CacheKey {
        let context = CompleteContext::new(result, ());
        let input_params = self.input_params
            .iter()
            .map(|&(pred, par)| context.get_parameter(pred, par).clone())
            .collect::<Vec<_>>();
        CacheKey {
            statement: self.statement.clone(),
            input_params: input_params,
        }
    }
}

impl Fetcher<CacheKey, CacheEntry> for SqlFetcher {
    fn fetch(&self, key: &CacheKey) -> CacheEntry {
        // TODO handle errors with Result<_, _>
        let start = UTC::now();
        let conn = self.pool.get().unwrap();
        let mut stmt = conn.prepare_cached(&self.statement).unwrap();
        let owned_params = self.input_params
            .iter()
            .map(|&(pred, par)| format!(":param{}x{}", pred, par))
            .zip(key.input_params.iter().map(to_sql_value))
            .collect::<Vec<_>>();
        let ref_params = owned_params.iter()
            .map(|&(ref name, ref value)| (name as &str, to_sql_ref(value)))
            .collect::<Vec<_>>();
        let value = match self.predicate.ty {
            PredicateType::OrderedStatic { .. } |
            PredicateType::UnorderedStatic { .. } => {
                if self.output_params.len() > 0 {
                    let cached = stmt.query_map_named(&ref_params, |row| {
                            self.output_params
                                .iter()
                                .enumerate()
                                .map(|(i, ty)| get_res(row, i as i32, ty))
                                .collect::<Vec<_>>()
                        })
                        .unwrap()
                        .flat_map(Result::unwrap)
                        .collect();
                    CacheEntryValue::Values(self.output_params.len(), cached)
                } else {
                    let cached =
                        stmt.query_map_named(&ref_params, |row| row.get::<_, i64>(0) as usize)
                            .unwrap()
                            .map(Result::unwrap)
                            .next()
                            .unwrap();
                    CacheEntryValue::Count(cached)
                }
            }
            PredicateType::StaticAggregate { .. } => {
                let value =
                    stmt.query_map_named(&ref_params,
                                         |row| get_res(row, 0, &self.output_params[0]))
                        .unwrap()
                        .map(Result::unwrap)
                        .next()
                        .unwrap();
                CacheEntryValue::Aggr(value)
            }
            PredicateType::StaticNegation { .. } => {
                let exists = stmt.query_named(&ref_params).unwrap().next().is_some();
                CacheEntryValue::Exists(exists)
            }
            _ => unreachable!(),
        };

        let cost = (UTC::now() - start)
            .num_nanoseconds()
            .map(|it| it as usize)
            .unwrap_or(usize::MAX);

        CacheEntry {
            cost: cost,
            value: value,
        }
    }
}

pub struct SQLiteDriver<C: SqlCache + ?Sized> {
    idx: usize,
    fetcher: CachedFetcher<C, SqlFetcher>,
}

impl<C: SqlCache + ?Sized> SQLiteDriver<C> {
    pub fn new(idx: usize,
               tuple: &TupleDeclaration,
               predicate: &Predicate,
               parameters_ty: &LinearMap<(usize, usize), BasicType>,
               pool: Pool<SqliteConnectionManager>,
               cache: Arc<Mutex<C>>)
               -> Option<Self> {
        if let TupleType::Static = tuple.ty {
            let mut input_params = predicate.get_used_parameters();
            input_params.retain(|&(param, _)| param != idx);
            let output_params = match predicate.ty {
                PredicateType::OrderedStatic { ref parameters, .. } |
                PredicateType::UnorderedStatic { ref parameters } => {
                    (0..parameters.len()).map(|n| parameters_ty[&(idx, n)].clone()).collect()
                }
                PredicateType::StaticAggregate { .. } => vec![parameters_ty[&(idx, 0)].clone()],
                _ => Vec::new(),
            };
            let statement = SqlContext::new(idx, tuple).encode_predicate(predicate);

            let fetcher = SqlFetcher {
                predicate: predicate.clone(),
                statement: statement,
                input_params: input_params,
                output_params: output_params,
                pool: pool,
            };

            Some(SQLiteDriver {
                idx: idx,
                fetcher: CachedFetcher::with_cache(cache, fetcher),
            })
        } else {
            None
        }
    }
}

// FIXME shouldn't be needed as soon as rusqlite is updated with the new ToSql trait
fn to_sql_value(value: &Value) -> SqlValue {
    match *value {
        Value::Int(x) => SqlValue::Integer(x.into()),
        Value::Float(x) => SqlValue::Real(x.into()),
        Value::Bool(x) => SqlValue::Integer(if x { 1 } else { 0 }),
        Value::Str(ref x) => SqlValue::Text(x.clone()),
    }
}

// FIXME shouldn't be needed as soon as rusqlite is updated with the new ToSql trait
fn to_sql_ref(value: &SqlValue) -> &ToSql {
    match *value {
        SqlValue::Integer(ref x) => x,
        SqlValue::Real(ref x) => x,
        SqlValue::Text(ref x) => x,
        _ => unreachable!(),
    }
}

fn get_res(row: &Row, i: i32, ty: &BasicType) -> Value {
    match *ty {
        BasicType::Int => Value::Int(row.get::<_, i64>(i) as i32),
        BasicType::Float => Value::Float(row.get::<_, f64>(i) as f32),
        BasicType::Bool => Value::Bool(row.get::<_, i64>(i) != 0),
        BasicType::Str => Value::Str(row.get(i)),
    }
}

impl<C: SqlCache + ?Sized> EventProcessor for SQLiteDriver<C> {
    fn evaluate(&self, result: &PartialResult) -> Vec<PartialResult> {
        // TODO Think a better way to prepare the key that doesn't require fetcher to be public
        let key = self.fetcher.fetcher.prepare_key(result);
        match (*self.fetcher.fetch(key)).value {
            CacheEntryValue::Values(chunk_size, ref cached) => {
                cached.chunks(chunk_size)
                    .map(|values| {
                        values.iter()
                            .enumerate()
                            .fold(result.clone(), |result, (i, value)| {
                                result.insert_parameter((self.idx, i), value.clone())
                            })
                    })
                    .collect()
            }
            CacheEntryValue::Count(count) => iter::repeat(result).cloned().take(count).collect(),
            CacheEntryValue::Aggr(ref value) => {
                vec![result.clone().insert_parameter((self.idx, 0), value.clone())]
            }
            CacheEntryValue::Exists(exists) => {
                if !exists { vec![result.clone()] } else { Vec::new() }
            }
        }
    }
}

// TODO Possible configurations:
// - Cache ownership: [per_predicate, per_rule, thread_local, shared]
// - Cache type: [dummy, random, lru, gdfs]
// - Cache size

#[derive(Debug, Copy, Clone)]
pub enum CacheOwnership {
    Shared,
    PerPredicate,
}

#[derive(Debug, Copy, Clone)]
pub enum CacheType {
    Dummy,
    Collision,
    Lru,
    Gdfs,
}

#[derive(Debug, Clone)]
pub struct SqliteConfig {
    pub db_file: String,
    pub pool_size: u32,
    pub cache_size: usize,
    pub cache_ownership: CacheOwnership,
    pub cache_type: CacheType,
}

fn make_cache(ty: CacheType,
              capacity: usize)
              -> Arc<Mutex<Cache<K = CacheKey, V = CacheEntry> + Send>> {
    match ty {
        CacheType::Dummy => Arc::new(Mutex::new(DummyCache::default())),
        CacheType::Collision => {
            let build_hasher: ModBuildHasher = ModBuildHasher::new(capacity as u64);
            let cache = CollisionCache::with_capacity_and_hasher(capacity, build_hasher);
            Arc::new(Mutex::new(cache))
        }
        CacheType::Lru => Arc::new(Mutex::new(LruCache::new(capacity))),
        CacheType::Gdfs => Arc::new(Mutex::<GDSFCache<_, _>>::new(GDSFCache::new(capacity))),
    }
}

pub struct SqliteProvider {
    pool: Pool<SqliteConnectionManager>,
    cache: Result<Arc<Mutex<Cache<K = CacheKey, V = CacheEntry> + Send>>, (CacheType, usize)>,
}

impl SqliteProvider {
    pub fn new(cfg: SqliteConfig) -> Self {
        let config = Config::builder().pool_size(cfg.pool_size).build();
        let manager = SqliteConnectionManager::new(&cfg.db_file);

        let cache = match cfg.cache_ownership {
            CacheOwnership::Shared => Ok(make_cache(cfg.cache_type, cfg.cache_size)),
            CacheOwnership::PerPredicate => Err((cfg.cache_type, cfg.cache_size)),
        };

        SqliteProvider {
            pool: Pool::new(config, manager).unwrap(),
            cache: cache,
        }
    }
}

impl NodeProvider for SqliteProvider {
    fn provide(&self,
               idx: usize,
               tuple: &TupleDeclaration,
               predicate: &Predicate,
               parameters_ty: &LinearMap<(usize, usize), BasicType>)
               -> Option<Box<EventProcessor>> {
        let cache = match self.cache {
            Ok(ref cache) => cache.clone(),
            Err((ty, capacity)) => make_cache(ty, capacity),
        };
        let pool = self.pool.clone();
        SQLiteDriver::new(idx, tuple, predicate, parameters_ty, pool, cache)
            .map(|it| Box::new(it) as Box<EventProcessor>)
    }
}
