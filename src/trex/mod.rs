mod expressions;
mod stacks;
mod sqldriver;

use tesla::{ClonableIterator, Engine, Event, EventsIterator, Listener, Rule, Tuple,
            TupleDeclaration};
use std::rc::Rc;
use std::collections::HashMap;

pub struct TRex {
    tuples: HashMap<usize, TupleDeclaration>,
    rules: Vec<Rule>,
    listeners: HashMap<usize, Box<Listener>>,
    last_id: usize,
}

impl TRex {
    pub fn new() -> TRex {
        TRex {
            tuples: HashMap::new(),
            rules: Vec::new(),
            listeners: HashMap::new(),
            last_id: 0,
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
        self.rules.push(rule);
    }
    fn publish(&mut self, event: &Rc<Event>) {
        for (_, listener) in &mut self.listeners {
            listener.receive(event);
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

pub type TuplesIterator<'a> = Box<ClonableIterator<'a, Item = &'a Rc<Tuple>> + 'a>;

trait Driver {
    fn evaluate<'a>(&mut self, context: ()) -> TuplesIterator<'a>;
}
