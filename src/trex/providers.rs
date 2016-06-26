use tesla::{Rule, TupleDeclaration};
use tesla::expressions::BasicType;
use tesla::predicates::Predicate;
use trex::FnvHashMap;
use trex::stacks::{EventProcessor, RuleStacks, Stack, Trigger};
use trex::sqldriver::SQLiteDriver;
use linear_map::LinearMap;
use r2d2::{Config, Pool};
use r2d2_sqlite::SqliteConnectionManager;

// TODO generalise provider architecture to allow custom nodes and node providers

trait NodeProvider {
    fn provide(&self,
               idx: usize,
               tuple: &TupleDeclaration,
               predicate: &Predicate,
               parameters_ty: &LinearMap<(usize, usize), BasicType>)
               -> Option<Box<EventProcessor>>;
}

struct StackProvider;

impl NodeProvider for StackProvider {
    fn provide(&self,
               idx: usize,
               tuple: &TupleDeclaration,
               predicate: &Predicate,
               _: &LinearMap<(usize, usize), BasicType>)
               -> Option<Box<EventProcessor>> {
        Stack::new(idx, tuple, predicate).map(Box::new).map(|it| it as Box<EventProcessor>)
    }
}

struct SqliteProvider {
    pool: Pool<SqliteConnectionManager>,
}

impl SqliteProvider {
    fn new() -> Self {
        let config = Config::builder().pool_size(10).build();
        let manager = SqliteConnectionManager::new("./database.db");
        SqliteProvider { pool: Pool::new(config, manager).unwrap() }
    }
}

impl NodeProvider for SqliteProvider {
    fn provide(&self,
               idx: usize,
               tuple: &TupleDeclaration,
               predicate: &Predicate,
               parameters_ty: &LinearMap<(usize, usize), BasicType>)
               -> Option<Box<EventProcessor>> {
        SQLiteDriver::new(idx, tuple, predicate, parameters_ty, self.pool.clone())
            .map(Box::new)
            .map(|it| it as Box<EventProcessor>)
    }
}

pub struct GeneralProvider {
    providers: Vec<Box<NodeProvider>>,
}

impl GeneralProvider {
    pub fn new() -> Self {
        GeneralProvider {
            providers: vec![
                Box::new(StackProvider),
                Box::new(SqliteProvider::new()),
            ],
        }
    }

    pub fn provide(&self,
                   rule: Rule,
                   tuples: &FnvHashMap<usize, TupleDeclaration>,
                   parameters_ty: &LinearMap<(usize, usize), BasicType>)
                   -> RuleStacks {
        let trigger = Trigger::new(&rule.predicates[0]);
        let processors = rule.predicates
            .iter()
            .enumerate()
            .skip(1)
            .map(|(i, predicate)| {
                let tuple = &tuples[&predicate.tuple.ty_id];
                let processor = self.providers
                    .iter()
                    .filter_map(|provider| provider.provide(i, tuple, &predicate, parameters_ty))
                    .next()
                    .expect("No suitable processor");
                (i, processor)
            })
            .collect();
        RuleStacks::new(trigger, processors, rule)
    }
}
