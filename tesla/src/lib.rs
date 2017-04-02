#![warn(missing_docs)]
// TODO general intro to the project

//! This crate contains the structs of the TESLA rule definition language AST
//! and the traits which describe the public API of a compatible CEP engine.

// TODO thorough explanation of the crate with meaningful examples

extern crate chrono;
extern crate ordered_float;

pub mod expressions;
pub mod predicates;

use chrono::{DateTime, UTC};
use expressions::{BasicType, Expression, Value};
use predicates::Predicate;
use std::sync::Arc;

/// Distinction between static data and event notifications.
#[derive(Clone, Debug)]
pub enum TupleType {
    /// Marks the tuple as originated from a persistent data source.
    Static,
    /// Marks the tuple as originated from an event stream.
    Event,
}

/// Declaration of a tuple attribute.
#[derive(Clone, Debug)]
pub struct AttributeDeclaration {
    /// Attribute name.
    pub name: String,
    /// Attribute basic type.
    pub ty: BasicType,
}

/// Declaration of a tuple.
#[derive(Clone, Debug)]
pub struct TupleDeclaration {
    /// Static data or event tuple.
    pub ty: TupleType,
    /// Unique numerical identifier for machine consumption.
    pub id: usize,
    /// Unique textual identifier for human consumption.
    pub name: String,
    /// List of the attributes that compose the tuple.
    pub attributes: Vec<AttributeDeclaration>,
}

/// Template to generate a new event from a successful rule evaluation.
#[derive(Clone, Debug)]
pub struct EventTemplate {
    /// Id of the corresponding tuple declaration.
    pub ty_id: usize,
    /// List of expressions to compute the attributes values.
    pub attributes: Vec<Expression>,
}

/// TESLA rule declaration.
#[derive(Clone, Debug)]
pub struct Rule {
    /// List of predicates that have to occur to trigger the rule.
    pub predicates: Vec<Predicate>,
    /// List of expressions that have to be satisfied to emit an event.
    pub filters: Vec<Expression>,
    /// Template to generate a new event tuple.
    pub event_template: EventTemplate,
    /// List of tuples declaration that are consumed after the event generation.
    pub consuming: Vec<usize>,
}

/// Generic tuple instance.
#[derive(Clone, Debug)]
pub struct Tuple {
    /// Id of the corresponding tuple declaration.
    pub ty_id: usize,
    /// List of attributes values.
    pub data: Vec<Value>,
}

/// Event tuple instance.
#[derive(Clone, Debug)]
pub struct Event {
    /// Generic tuple info.
    pub tuple: Tuple,
    /// Occurrence time.
    pub time: DateTime<UTC>,
}

/// Subscription filter to specify the listener interests.
#[derive(Clone, Debug)]
pub enum SubscrFilter {
    /// Do not filter.
    Any,
    /// Filter by event type.
    Topic {
        /// Id of the desired tuple declaration.
        ty: usize
    },
    /// Filter by event type and content evaluation.
    Content {
        /// Id of the desired tuple declaration.
        ty: usize,
        /// List of boolean expressions to evaluate the tuple.
        filters: Vec<Expression>
    },
}

/// Describes the engine subscribers.
pub trait Listener {
    /// Processes an event notifications.
    ///
    /// The method is called by the engine to notify the subscriber
    /// whenever there is a new event that satisfy the subscription filters.
    fn receive(&mut self, event: &Arc<Event>);
}

/// Describes a TESLA compatible CEP engine.
pub trait Engine {
    /// Declares a tuple.
    ///
    /// # Note
    ///
    /// To ensure rule meaningfulness and type checking,
    /// tuples need to be declared before their usage.
    fn declare(&mut self, tuple: TupleDeclaration);
    /// Defines a rule.
    fn define(&mut self, rule: Rule);
    /// Publishes an event.
    fn publish(&mut self, event: &Arc<Event>);
    /// Registers a listener with the given filters and returns a subscription identifier.
    fn subscribe(&mut self, condition: SubscrFilter, listener: Box<Listener>) -> usize;
    /// Remove a listener given the corresponding subscription identifier.
    fn unsubscribe(&mut self, listener_id: usize);
}
