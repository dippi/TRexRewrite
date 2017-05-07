#![warn(missing_docs)]

//! ## TESLA language
//!
//! *TESLA*[^1] is a declarative strongly typed *Complex Event Processing* (CEP) rule definition language.
//!
//! It provides a comprehensive set of common operations on events (like filtering and
//! parameterization, composition and pattern detection, negation and aggregation) and allows
//! to control selection policies, time windows and event consumption.
//!
//! However, while the other alternative languages rely on informal documentation that leaves room
//! to ambiguities, TESLA’s unprecedented characteristic is its aim to a complete semantic
//! specification with *TRIO*[^2], a first order temporal logic.
//! The definition of a precise behavior for each feature improves coherence in the development
//! of engines based on TESLA and helps users to understand the language deeply with less
//! empirical research.
//!
//! This crate is part of the code developed for my thesis[^3], that includes some modification
//! and refinement of the original syntax and semantic, plus a native and general purpose
//! integration of static data sources.
//!
// TODO Add links to pdf files
//!
//! [^1]: Gianpaolo Cugola and Alessandro Margara.
//!       "TESLA: a formally defined event specification language".
//!       In: Proceedings of the Fourth ACM International Conference
//!       on Distributed Event-Based Systems. ACM. 2010, pp. 50–61.
//!
//! [^2]: Carlo Ghezzi, Dino Mandrioli, and Angelo Morzenti.
//!       "TRIO: A logic language for executable specifications of real-time systems".
//!       In: Journal of Systems and software 12.2 (1990), pp. 107– 123.
//!
//! [^3]: Angelo Di Pilla with the supervision of Gianpaolo Cugola and Alessandro Margara
//!       "Combining streaming events with static data in the Complex Event Processing tool T-Rex".
//!       Thesis of master degree in Computer Science and Engineering
//!       at the Polytechnic University of Milan (2016).
//!
// TODO think if it's worth it to transcribe some of the info about the language from the papers
// TODO thorough explanation of the crate with meaningful examples
//! ## Content of the crate
//!
//! This crate contains the structs that compose the TESLA abstract syntax tree (AST)
//! and the traits that describe the public API of a compatible CEP engine implementation.

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
