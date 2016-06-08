pub mod expressions;
pub mod predicates;

use std::rc::Rc;
use std::boxed::Box;
use std::vec::Vec;
use std::string::String;
use std::option::Option;
use std::result::Result;
use chrono::{DateTime, UTC};
use self::expressions::{BasicType, Expression, Value};
use self::predicates::Predicate;

#[derive(Clone, Debug)]
pub enum TupleType {
    Static,
    Event,
}

#[derive(Clone, Debug)]
pub struct AttributeDeclaration {
    pub name: String,
    pub ty: BasicType,
}

#[derive(Clone, Debug)]
pub struct TupleDeclaration {
    pub ty: TupleType,
    pub id: usize, // TODO maybe remove it and let the method `declare` return an id
    pub name: String,
    pub attributes: Vec<AttributeDeclaration>,
}

#[derive(Clone, Debug)]
pub struct EventTemplate {
    pub ty_id: usize,
    pub attributes: Vec<Expression>,
}

#[derive(Clone, Debug)]
pub struct Rule {
    predicates: Vec<Predicate>,
    filters: Vec<Expression>,
    event_template: EventTemplate,
    consuming: Vec<usize>,
}

impl Rule {
    pub fn new(predicates: Vec<Predicate>,
               filters: Vec<Expression>,
               event_template: EventTemplate,
               consuming: Vec<usize>)
               -> Result<Rule, String> {
        // TODO check for rule validity
        // (Parameters definition and usage,
        // aggregate expr can appear only
        // in aggregate parameter definitions,
        // and consuming ranges validity)
        Ok(Rule {
            predicates: predicates,
            filters: filters,
            event_template: event_template,
            consuming: consuming,
        })
    }
    pub fn predicates(&self) -> &Vec<Predicate> {
        &self.predicates
    }
    pub fn filters(&self) -> &Vec<Expression> {
        &self.filters
    }
    pub fn event_template(&self) -> &EventTemplate {
        &self.event_template
    }
    pub fn consuming(&self) -> &Vec<usize> {
        &self.consuming
    }
}

#[derive(Clone, Debug)]
pub struct Tuple {
    pub ty_id: usize,
    pub data: Vec<Value>,
}

#[derive(Clone, Debug)]
pub struct Event {
    pub tuple: Tuple,
    pub time: DateTime<UTC>,
}

pub trait ClonableIterator<'a>: Iterator {
    fn clone_iter(&self) -> Box<ClonableIterator<'a, Item = Self::Item> + 'a>;
}

impl<'a, T> ClonableIterator<'a> for T
    where T: Iterator + Clone + 'a
{
    fn clone_iter(&self) -> Box<ClonableIterator<'a, Item = Self::Item> + 'a> {
        Box::new(self.clone())
    }
}

impl<'a, T: 'a> Clone for Box<ClonableIterator<'a, Item = T> + 'a> {
    fn clone(&self) -> Self {
        (**self).clone_iter()
    }
}

pub type EventsIterator<'a> = Box<ClonableIterator<'a, Item = &'a Rc<Event>> + 'a>;

pub trait Listener {
    fn receive(&mut self, event: &Rc<Event>);
    fn receive_all(&mut self, events: EventsIterator) {
        for event in events {
            self.receive(event);
        }
    }
}

pub trait Engine {
    fn declare(&mut self, tuple: TupleDeclaration);
    fn define(&mut self, rule: Rule);
    fn publish(&mut self, event: &Rc<Event>);
    fn publish_all(&mut self, events: EventsIterator) {
        for event in events {
            self.publish(event);
        }
    }
    fn subscribe(&mut self, listener: Box<Listener>) -> usize;
    fn unsubscribe(&mut self, listener_id: &usize) -> Option<Box<Listener>>;
}
