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

// TODO maybe refactor Rule fields names
// (sometimes they conflict with common traits or language words)
// Maybe: predicates, filters, event_template and consuming

#[derive(Clone, Debug)]
pub struct Rule {
    from: Vec<Predicate>,
    when: Vec<Expression>,
    emit: EventTemplate,
    consuming: Vec<usize>,
}

impl Rule {
    pub fn from(&self) -> &Vec<Predicate> {
        &self.from
    }
    pub fn when(&self) -> &Vec<Expression> {
        &self.when
    }
    pub fn emit(&self) -> &EventTemplate {
        &self.emit
    }
    pub fn consuming(&self) -> &Vec<usize> {
        &self.consuming
    }
}

#[derive(Clone, Debug)]
pub struct RuleBuilder {
    from: Option<Vec<Predicate>>,
    when: Option<Vec<Expression>>,
    emit: Option<EventTemplate>,
    consuming: Option<Vec<usize>>,
}

impl RuleBuilder {
    pub fn new() -> RuleBuilder {
        RuleBuilder {
            from: None,
            when: None,
            emit: None,
            consuming: None,
        }
    }
    pub fn from_rule(rule: Rule) -> RuleBuilder {
        // TODO implement trait From
        RuleBuilder {
            from: Some(rule.from),
            when: Some(rule.when),
            emit: Some(rule.emit),
            consuming: Some(rule.consuming),
        }
    }
    pub fn from(&mut self, predicates: Vec<Predicate>) -> &mut RuleBuilder {
        self.from = Some(predicates);
        self
    }
    pub fn when(&mut self, filters: Vec<Expression>) -> &mut RuleBuilder {
        self.when = Some(filters);
        self
    }
    pub fn emit(&mut self, template: EventTemplate) -> &mut RuleBuilder {
        self.emit = Some(template);
        self
    }
    pub fn consuming(&mut self, events: Vec<usize>) -> &mut RuleBuilder {
        self.consuming = Some(events);
        self
    }
    pub fn finalize(self) -> Result<Rule, String> {
        if let RuleBuilder { from: Some(from),
                             when: Some(when),
                             emit: Some(emit),
                             consuming: Some(consuming) } = self {
            // TODO check for rule validity
            // (Parameters definition and usage,
            // aggregate expr can appear only
            // in aggregate parameter definitions,
            // and consuming ranges validity)
            Ok(Rule {
                from: from,
                when: when,
                emit: emit,
                consuming: consuming,
            })
        } else {
            // TODO improve error management
            Err("Uncomplete!".to_owned())
        }
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
    fn receive_all<'a>(&mut self, events: EventsIterator<'a>) {
        for event in events {
            self.receive(event);
        }
    }
}

pub trait Engine {
    fn declare(&mut self, tuple: TupleDeclaration);
    fn define(&mut self, rule: Rule);
    fn publish(&mut self, event: &Rc<Event>);
    fn publish_all<'a>(&mut self, events: EventsIterator<'a>) {
        for event in events {
            self.publish(event);
        }
    }
    fn subscribe(&mut self, listener: Box<Listener>) -> usize;
    fn unsubscribe(&mut self, listener_id: &usize) -> Option<Box<Listener>>;
}
