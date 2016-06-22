mod expressions;
mod aggregators;
mod stacks;
mod operations;
mod sqldriver;

use tesla::{ClonableIterator, Engine, Event, EventsIterator, Listener, Rule, Tuple,
            TupleDeclaration};
use std::collections::{BTreeMap, HashMap};
use std::hash::BuildHasherDefault;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::mpsc::{Receiver, Sender, channel};
use threadpool::ThreadPool;
use fnv::FnvHasher;
use self::stacks::*;

pub type FnvHashMap<K, V> = HashMap<K, V, BuildHasherDefault<FnvHasher>>;

pub struct TRex {
    tuples: FnvHashMap<usize, TupleDeclaration>,
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
        self.tuples.insert(tuple.id, tuple);
    }
    fn define(&mut self, rule: Rule) {
        // TODO check for rule validity
        // (predicates type and tuple they refers to,
        // expressions refereces ranges and types,
        // emit complete assignment and correct types)
        let mut pred_ty_ids =
            rule.predicates().iter().map(|pred| pred.tuple.ty_id).collect::<Vec<_>>();
        pred_ty_ids.sort();
        pred_ty_ids.dedup();

        let stack = Arc::new(Mutex::new(RuleStacks::new(rule, &self.tuples)));
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

pub type TuplesIterator<'a> = Box<ClonableIterator<'a, Item = &'a Arc<Tuple>> + 'a>;

trait Driver {
    // fn new(predicate: &Predicate, tuple: &TupleDeclaration) -> Self;
    fn evaluate<'a>(&mut self, context: ()) -> TuplesIterator<'a>;
}
