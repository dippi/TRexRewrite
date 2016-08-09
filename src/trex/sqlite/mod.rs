mod query_builder;

use tesla::*;
use tesla::expressions::*;
use tesla::predicates::*;
use trex::NodeProvider;
use trex::rule_processor::*;
use trex::expressions::evaluation::*;
use trex::cache::{Cache, CachedFetcher, Fetcher};
use self::query_builder::SqlContext;
use linear_map::LinearMap;
use lru_cache::LruCache;
use rusqlite::Row;
use rusqlite::types::{ToSql, Value as SqlValue};
use r2d2::{Config, Pool};
use r2d2_sqlite::SqliteConnectionManager;
use std::sync::{Arc, Mutex};

#[derive(Debug, Hash, PartialEq, Eq)]
pub struct CacheKey {
    statement: String,
    input_params: Vec<Value>,
}

#[derive(Debug)]
pub enum CacheEntry {
    Values(Vec<Vec<Value>>),
    Exists(bool),
}

pub trait SqlCache: Cache<K = CacheKey, V = CacheEntry> + Send {}
impl<T: Cache<K = CacheKey, V = CacheEntry> + Send> SqlCache for T {}

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
        match self.predicate.ty {
            PredicateType::OrderdStatic { .. } |
            PredicateType::UnorderedStatic { .. } => {
                let cached = stmt.query_map_named(&ref_params, |row| {
                        self.output_params
                            .iter()
                            .enumerate()
                            .map(|(i, ty)| get_res(row, i as i32, ty))
                            .collect()
                    })
                    .unwrap()
                    .map(Result::unwrap)
                    .collect();
                CacheEntry::Values(cached)
            }
            PredicateType::StaticAggregate { .. } => {
                let values =
                    stmt.query_map_named(&ref_params,
                                         |row| get_res(row, 1, &self.output_params[0]))
                        .unwrap()
                        .map(Result::unwrap)
                        .collect();
                CacheEntry::Values(vec![values])
            }
            PredicateType::StaticNegation { .. } => {
                let exists = stmt.query_named(&ref_params).unwrap().next().is_some();
                CacheEntry::Exists(exists)
            }
            _ => unreachable!(),
        }
    }
}

pub struct SQLiteDriver<C: SqlCache> {
    idx: usize,
    fetcher: CachedFetcher<C, SqlFetcher>,
}

impl<C: SqlCache> SQLiteDriver<C> {
    pub fn new(idx: usize,
               tuple: &TupleDeclaration,
               predicate: &Predicate,
               parameters_ty: &LinearMap<(usize, usize), BasicType>,
               pool: Pool<SqliteConnectionManager>,
               cache: Arc<Mutex<C>>)
               -> Option<Self> {
        if let TupleType::Static = tuple.ty {
            let mut input_params = predicate.tuple
                .constraints
                .iter()
                .flat_map(|expr| expr.get_parameters())
                .filter(|&(param, _)| param != idx)
                .collect::<Vec<_>>();
            input_params.sort();
            input_params.dedup();
            let output_params = match predicate.ty {
                PredicateType::OrderdStatic { ref parameters, .. } |
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

impl<C: SqlCache> EventProcessor for SQLiteDriver<C> {
    fn evaluate(&self, result: &PartialResult) -> Vec<PartialResult> {
        // TODO Think a better way to prepare the key that doesn't require fetcher to be public
        let key = self.fetcher.fetcher.prepare_key(result);
        match *self.fetcher.fetch(key) {
            CacheEntry::Values(ref cached) => {
                cached.iter()
                    .map(|values| {
                        values.iter()
                            .enumerate()
                            .fold(result.clone(), |result, (i, value)| {
                                result.insert_parameter((self.idx, i), value.clone())
                            })
                    })
                    .collect()
            }
            CacheEntry::Exists(exists) => if !exists { vec![result.clone()] } else { Vec::new() },
        }
    }
}

pub struct SqliteProvider {
    pool: Pool<SqliteConnectionManager>,
    cache: Arc<Mutex<LruCache<CacheKey, CacheEntry>>>,
}

impl SqliteProvider {
    pub fn new() -> Self {
        let config = Config::builder().pool_size(10).build();
        let manager = SqliteConnectionManager::new("./database.db");
        SqliteProvider {
            pool: Pool::new(config, manager).unwrap(),
            cache: Arc::new(Mutex::new(LruCache::new(100))),
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
        SQLiteDriver::new(idx,
                          tuple,
                          predicate,
                          parameters_ty,
                          self.pool.clone(),
                          self.cache.clone())
            .map(Box::new)
            .map(|it| it as Box<EventProcessor>)
    }
}
