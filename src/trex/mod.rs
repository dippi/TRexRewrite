mod expressions;
mod aggregators;
mod stacks;
mod operations;
mod sqldriver;

use tesla::{ClonableIterator, Engine, Event, EventsIterator, Listener, Rule, Tuple,
            TupleDeclaration};
use std::rc::Rc;
use std::collections::HashMap;
use self::stacks::*;

pub struct TRex {
    tuples: HashMap<usize, TupleDeclaration>,
    stacks: Vec<RuleStacks>,
    listeners: HashMap<usize, Box<Listener>>,
    last_id: usize,
}

impl TRex {
    pub fn new() -> TRex {
        TRex {
            tuples: HashMap::new(),
            stacks: Vec::new(),
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
        self.stacks.push(RuleStacks::new(rule, &self.tuples));
    }
    fn publish(&mut self, event: &Rc<Event>) {
        for (_, listener) in &mut self.listeners {
            listener.receive(event);
        }
        let mut events = Vec::new();
        for stack in &mut self.stacks {
            events.append(&mut stack.process(event));
        }
        for event in &events {
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

pub type TuplesIterator<'a> = Box<ClonableIterator<'a, Item = &'a Rc<Tuple>> + 'a>;

trait Driver {
    fn evaluate<'a>(&mut self, context: ()) -> TuplesIterator<'a>;
}
