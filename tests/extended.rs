// TODO Parameters:
// * Rule
//   - Num of predicates
//   - Order of predicates
// * Events
//   - Frequency
// * Static
//   - Num of rows
// * Data (both event and static)
//   - Num of attributes / columns
//   - Type of data
//   - Data domain
//   - Repetitions
// * Events queries
//   - Window
// * Static queries
//   - Load time
// * Queries (both event and static)
//   - Num of input params
//   - Num of output params
//   - Selection policy
//   - Num of results
//   - Selectivity (#propagated / #processed OR #results / #rows)
//   - Variations of num of results
//   - Filters complexity
//   - Aggregates complexity
// * Cache
//   - Size
//   - Type
//   - Ownership
//   - Ratio repeated vs new
//   - Hit time vs miss time
// * Other
//   - Pre fetching
//   - SQL Indexes

#![feature(step_by, inclusive_range_syntax)]

extern crate trex;
extern crate chrono;
extern crate rand;
extern crate rusqlite;

use chrono::{Duration, UTC};
use rand::Rng;
use rusqlite::Connection;
use rusqlite::types::ToSql;
use std::iter::{once, repeat};
use std::ops::Add;
use std::sync::Arc;
use std::sync::mpsc::sync_channel;
use std::thread;
use trex::tesla::{AttributeDeclaration, Engine, Event, EventTemplate, Rule, Tuple,
                  TupleDeclaration, TupleType};
use trex::tesla::expressions::*;
use trex::tesla::predicates::*;
use trex::trex::*;
use trex::trex::sqlite::{CacheOwnership, CacheType, SqliteConfig, SqliteProvider};
use trex::trex::stack::StackProvider;

struct Config {
    num_rules: usize,
    num_def: usize,
    num_pred: usize,
    num_events: usize,
    each_prob: f32,
    first_prob: f32,
    min_win: Duration,
    max_win: Duration,
    consuming: bool,
    queue_len: usize,
    evts_per_sec: usize,
    db_name: String,
    table_columns: usize,
    table_rows: usize,
    cache_size: usize,
    cache_ownership: CacheOwnership,
    cache_type: CacheType,
    matching_rows: usize,
    matching_range: usize,
}

fn setup_db<R: Rng>(rng: &mut R, cfg: &Config) {
    // Open database (create if not exists)
    let mut conn = Connection::open(&cfg.db_name).unwrap();
    let tx = conn.transaction().unwrap();

    {
        // Drop and recreate the table
        tx.execute("DROP TABLE test", &[]).unwrap_or(0);

        let columns = (0..cfg.table_columns).fold(String::new(), |acc, i| {
            acc + &format!(", col{} INTEGER NOT NULL", i)
        });
        let create_query = format!("CREATE TABLE test (id INTEGER PRIMARY KEY{})", columns);
        tx.execute(&create_query, &[]).unwrap();

        // generate fill data
        let columns = (0..cfg.table_columns)
            .fold(String::new(), |acc, i| acc + &format!(", col{}", i));
        let placeholders = repeat(", ?").take(cfg.table_columns).fold(String::new(), Add::add);
        let insert_query = format!("INSERT INTO test (id{}) VALUES (?{})",
                                   columns,
                                   placeholders);
        let mut stmt = tx.prepare(&insert_query).unwrap();

        for i in 0..cfg.table_rows {
            let data: Vec<_> = once(i as i64)
                .chain(repeat(rng.gen_range(0i64, cfg.table_rows as i64)).take(cfg.table_columns))
                .collect();
            let reference: Vec<_> = data.iter().map(|it| it as &ToSql).collect();
            stmt.execute(&reference).unwrap();
        }
    }

    tx.commit().unwrap();
}

fn generate_declarations<R: Rng>(rng: &mut R, cfg: &Config) -> Vec<TupleDeclaration> {
    (0..cfg.num_def)
        .flat_map(|i| {
            let id = i + 1;
            let output_decl = TupleDeclaration {
                ty: TupleType::Event,
                id: id,
                name: format!("event{}", id),
                attributes: Vec::new(),
            };
            let attrs = (0..3)
                .map(|j| {
                    AttributeDeclaration {
                        name: format!("attr{}", j),
                        ty: BasicType::Int,
                    }
                })
                .collect();
            let root_decl = TupleDeclaration {
                ty: TupleType::Event,
                id: id * 1000,
                name: format!("event{}", id * 1000),
                attributes: attrs,
            };
            let mid_decls = (1..(cfg.num_pred - 1)).map(move |j| {
                let attr = AttributeDeclaration {
                    name: "attr".to_owned(),
                    ty: BasicType::Int,
                };
                TupleDeclaration {
                    ty: TupleType::Event,
                    id: id * 1000 + j,
                    name: format!("event{}", id * 1000 + j),
                    attributes: vec![attr],
                }
            });
            let attributes = (0..cfg.table_columns)
                .map(|i| {
                    AttributeDeclaration {
                        name: format!("col{}", i),
                        ty: BasicType::Int,
                    }
                })
                .collect();
            let static_decl = TupleDeclaration {
                ty: TupleType::Static,
                id: id * 1000 + cfg.num_pred - 1,
                name: "test".to_owned(),
                attributes: attributes,
            };
            once(output_decl).chain(once(root_decl)).chain(mid_decls).chain(once(static_decl))
        })
        .collect()
}

fn generate_rules<R: Rng>(rng: &mut R, cfg: &Config) -> Vec<Rule> {
    (0..cfg.num_rules)
        .map(|i| {
            let id = i % cfg.num_def + 1;
            let constraint = Arc::new(Expression::BinaryOperation {
                operator: BinaryOperator::Equal,
                left: Box::new(Expression::Reference { attribute: 0 }),
                right: Box::new(Expression::Immediate { value: 1.into() }),
            });
            let root_parameter1 = ParameterDeclaration {
                name: "x".to_owned(),
                expression: Arc::new(Expression::Reference { attribute: 1 }),
            };
            let root_parameter2 = ParameterDeclaration {
                name: "y".to_owned(),
                expression: Arc::new(Expression::Reference { attribute: 2 }),
            };
            let root_pred = Predicate {
                ty: PredicateType::Trigger { parameters: vec![root_parameter1, root_parameter2] },
                tuple: ConstrainedTuple {
                    ty_id: id * 1000,
                    constraints: vec![constraint.clone()],
                    alias: format!("alias{}", id * 1000),
                },
            };
            let mid_preds = (1..(cfg.num_pred - 1)).map(|j| {
                let rand = rng.next_f32();
                let selection = if rand < cfg.each_prob {
                    EventSelection::Each
                } else if rand < cfg.each_prob + cfg.first_prob {
                    EventSelection::First
                } else {
                    EventSelection::Last
                };
                let millis = rng.gen_range(cfg.min_win.num_milliseconds(),
                                           cfg.max_win.num_milliseconds());
                let timing = Timing {
                    upper: j - 1,
                    bound: TimingBound::Within { window: Duration::milliseconds(millis) },
                };
                Predicate {
                    ty: PredicateType::Event {
                        selection: selection,
                        parameters: Vec::new(),
                        timing: timing,
                    },
                    tuple: ConstrainedTuple {
                        ty_id: id * 1000 + j,
                        constraints: vec![constraint.clone()],
                        alias: format!("alias{}", id * 1000 + j),
                    },
                }
            });
            let static_constr1 = Arc::new(Expression::BinaryOperation {
                operator: BinaryOperator::GreaterEqual,
                left: Box::new(Expression::Reference { attribute: 0 }),
                right: Box::new(Expression::Parameter {
                    predicate: 0,
                    parameter: 0,
                }),
            });
            let static_constr2 = Arc::new(Expression::BinaryOperation {
                operator: BinaryOperator::LowerThan,
                left: Box::new(Expression::Reference { attribute: 0 }),
                right: Box::new(Expression::Parameter {
                    predicate: 0,
                    parameter: 1,
                }),
            });
            let parameters = (0..cfg.table_columns)
                .map(|i| {
                    ParameterDeclaration {
                        name: format!("z{}", i),
                        expression: Arc::new(Expression::Reference { attribute: i }),
                    }
                })
                .collect();
            let static_pred = Predicate {
                ty: PredicateType::UnorderedStatic { parameters: parameters },
                tuple: ConstrainedTuple {
                    ty_id: id * 1000 + cfg.num_pred - 1,
                    constraints: vec![static_constr1, static_constr2],
                    alias: format!("alias{}", id * 1000 + cfg.num_pred),
                },
            };
            let predicates = once(root_pred).chain(mid_preds).chain(once(static_pred)).collect();
            let event_template = EventTemplate {
                ty_id: id,
                attributes: Vec::new(),
            };
            let consuming = if cfg.consuming { vec![1] } else { Vec::new() };
            Rule {
                predicates: predicates,
                filters: Vec::new(),
                event_template: event_template,
                consuming: consuming,
            }
        })
        .collect()
}

fn generate_events<R: Rng>(rng: &mut R, cfg: &Config) -> Vec<Event> {
    (0..cfg.num_events)
        .map(|_| {
            let def = rng.gen_range(0, cfg.num_def) + 1;
            let state = rng.gen_range(0, cfg.num_pred - 1);
            if state == 0 {
                let lower_bound = rng.gen_range(0, cfg.matching_range) as i32;
                let upper_bound =
                    lower_bound +
                    ((cfg.matching_rows as f32) * rng.choose(&[0.5, 1.0, 1.5]).unwrap()) as i32;
                Event {
                    tuple: Tuple {
                        ty_id: def * 1000,
                        data: vec![Value::Int(1), lower_bound.into(), upper_bound.into()],
                    },
                    time: UTC::now(),
                }
            } else {
                Event {
                    tuple: Tuple {
                        ty_id: def * 1000 + state,
                        data: vec![Value::Int(1)],
                    },
                    time: UTC::now(),
                }
            }
        })
        .collect()
}

fn execute_bench(cfg: &Config) {
    let mut rng = rand::thread_rng();
    setup_db(&mut rng, cfg);
    let decls = generate_declarations(&mut rng, cfg);
    let rules = generate_rules(&mut rng, cfg);
    let evts = generate_events(&mut rng, cfg);

    let sqlite_config = SqliteConfig {
        db_file: cfg.db_name.clone(),
        pool_size: 10,
        cache_size: cfg.cache_size,
        cache_ownership: cfg.cache_ownership,
        cache_type: cfg.cache_type,
    };
    let sqlite_provider = Box::new(SqliteProvider::new(sqlite_config));
    let providers: Vec<Box<NodeProvider>> = vec![Box::new(StackProvider), sqlite_provider];

    let mut engine = TRex::new(providers);
    for decl in decls {
        engine.declare(decl);
    }
    for rule in rules {
        engine.define(rule);
    }

    use trex::trex::listeners::{CountListener, DebugListener};
    // engine.subscribe(Box::new(DebugListener));
    engine.subscribe(Box::new(CountListener {
        count: 0,
        duration: cfg.num_events / cfg.evts_per_sec,
    }));

    let start = UTC::now();

    let (tx, rx) = sync_channel(cfg.queue_len);
    let evts_per_sec = cfg.evts_per_sec as u32;
    let thr = thread::spawn(move || {
        let mut dropped = 0;
        for mut evt in evts {
            evt.time = UTC::now();
            if tx.try_send(evt).is_err() {
                dropped += 1;
            }
            thread::sleep(std::time::Duration::new(0, 1000_000_000 / evts_per_sec));
        }
        dropped
    });
    while let Ok(evt) = rx.recv() {
        engine.publish(&Arc::new(evt));
    }

    println!("Dropped: {:2.2}% - Time: {:5}ms",
             thr.join().unwrap() as f32 / cfg.num_events as f32 * 100.0,
             (UTC::now() - start).num_milliseconds());
}

#[test]
fn extended_test() {
    println!("");
    for freq in (5_000...5_000).step_by(2000) {
        println!("- Frequency: {:5} evt/sec", freq);
        for avg_win in (2...2).step_by(4) {
            let max_win = Duration::seconds(avg_win + 1 as i64);
            let min_win = Duration::seconds(avg_win - 1 as i64);
            print!(" > Avg Window: {:2}s => ", avg_win);

            let mut cfg = Config {
                num_rules: 1000,
                num_def: 100,
                num_pred: 4,
                num_events: 30_000,
                each_prob: 1.0,
                first_prob: 0.0,
                min_win: min_win,
                max_win: max_win,
                consuming: false,
                queue_len: 100,
                evts_per_sec: freq,
                db_name: "./database.db".to_owned(),
                table_columns: 2,
                table_rows: 100_000,
                cache_size: 1000,
                cache_ownership: CacheOwnership::PerPredicate,
                cache_type: CacheType::Lru,
                matching_rows: 10,
                matching_range: 300,
            };

            execute_bench(&cfg);
        }
    }
}
