mod expressions;
mod stack;
mod rule_processor;
mod aggregators;
mod sqlite;
mod rule_checks;

use tesla::{Engine, Event, Listener, Rule, TupleDeclaration};
use tesla::predicates::Predicate;
use tesla::expressions::BasicType;
use std::collections::{BTreeMap, HashMap};
use std::collections::hash_map::Entry;
use std::hash::BuildHasherDefault;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::mpsc::{Receiver, Sender, channel};
use threadpool::ThreadPool;
use linear_map::LinearMap;
use fnv::FnvHasher;
use trex::rule_checks::check_rule;
use trex::rule_processor::*;
use trex::stack::StackProvider;
use trex::sqlite::SqliteProvider;

pub type FnvHashMap<K, V> = HashMap<K, V, BuildHasherDefault<FnvHasher>>;

trait NodeProvider {
    fn provide(&self,
               idx: usize,
               tuple: &TupleDeclaration,
               predicate: &Predicate,
               parameters_ty: &LinearMap<(usize, usize), BasicType>)
               -> Option<Box<EventProcessor>>;
}

struct GeneralProvider {
    providers: Vec<Box<NodeProvider>>,
}

impl GeneralProvider {
    fn new() -> Self {
        // TODO generalise provider architecture to allow custom nodes and node providers
        GeneralProvider {
            providers: vec![
                Box::new(StackProvider),
                Box::new(SqliteProvider::new()),
            ],
        }
    }

    fn provide(&self,
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

pub struct TRex {
    tuples: FnvHashMap<usize, TupleDeclaration>,
    provider: GeneralProvider,
    reverse_index: FnvHashMap<usize, Vec<Arc<Mutex<RuleStacks>>>>,
    listeners: BTreeMap<usize, Box<Listener>>,
    last_id: usize,
    threadpool: ThreadPool,
    channel: (Sender<Vec<Arc<Event>>>, Receiver<Vec<Arc<Event>>>),
}

impl TRex {
    pub fn new() -> TRex {
        TRex {
            tuples: FnvHashMap::default(),
            provider: GeneralProvider::new(),
            reverse_index: FnvHashMap::default(),
            listeners: BTreeMap::new(),
            last_id: 0,
            threadpool: ThreadPool::new(4),
            channel: channel(),
        }
    }
}

impl Engine for TRex {
    fn declare(&mut self, tuple: TupleDeclaration) {
        if let Entry::Vacant(entry) = self.tuples.entry(tuple.id) {
            entry.insert(tuple);
        } else {
            panic!("Tuple already declared!");
        }
    }
    fn define(&mut self, rule: Rule) {
        // TODO handle error with result
        let param_types = check_rule(&rule, &self.tuples).unwrap();

        let mut pred_ty_ids =
            rule.predicates.iter().map(|pred| pred.tuple.ty_id).collect::<Vec<_>>();
        pred_ty_ids.sort();
        pred_ty_ids.dedup();

        let stack = Arc::new(Mutex::new(self.provider.provide(rule, &self.tuples, &param_types)));
        for idx in pred_ty_ids {
            self.reverse_index.entry(idx).or_insert_with(Vec::new).push(stack.clone());
        }

    }
    fn publish(&mut self, event: &Arc<Event>) {
        for (_, listener) in &mut self.listeners {
            listener.receive(event);
        }

        let events = {
            let (ref tx, ref rx) = self.channel;
            let empty = Vec::new();
            let stacks = self.reverse_index.get(&event.tuple.ty_id).unwrap_or(&empty);
            for stack in stacks {
                let tx = tx.clone();
                let stack = stack.clone();
                let event = event.clone();
                self.threadpool.execute(move || {
                    let mut stack = stack.lock().unwrap();
                    tx.send(stack.process(&event)).unwrap()
                });
            }
            rx.iter().take(stacks.len()).collect::<Vec<_>>()
        };

        for event in events.iter().flat_map(|it| it) {
            // TODO change recursion into loop and detect infinite loop
            self.publish(event)
        }
    }
    fn subscribe(&mut self, listener: Box<Listener>) -> usize {
        self.last_id += 1;
        self.listeners.insert(self.last_id, listener);
        self.last_id
    }
    fn unsubscribe(&mut self, listener_id: &usize) -> Option<Box<Listener>> {
        self.listeners.remove(listener_id)
    }
}
