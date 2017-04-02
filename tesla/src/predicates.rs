//! TESLA predicates definition.
//!
//! This module contains the structures that represent the rule predicates.

use chrono::Duration;
use expressions::Expression;
use std::string::String;
use std::vec::Vec;

/// Tuple selection for event predicates.
#[derive(Clone, Debug)]
pub enum EventSelection {
    /// Select each event.
    Each,
    /// Select the least recent event.
    First,
    /// Select the most recent event.
    Last,
}

/// Aggregate functions.
///
/// The aggregate can be characterized by an index of the attribute on which it will be executed.
#[derive(Clone, Debug)]
pub enum Aggregator {
    /// Average of the values.
    Avg(usize),
    /// Sum of the values.
    Sum(usize),
    /// Maximum value.
    Max(usize),
    /// Minimum value.
    Min(usize),
    /// Count of the elements.
    Count,
    // TODO add ANY and ALL?
}

/// Declaration of a new parameter.
#[derive(Clone, Debug)]
pub struct ParameterDeclaration {
    /// Name of the parameter.
    pub name: String,
    /// Expression used to compute the parameter value.
    pub expression: Expression,
}

/// Lower bound of the time window of an event predicate.
#[derive(Clone, Debug)]
pub enum TimingBound {
    /// Lower bound given by fixed duration.
    Within {
        /// Fixed time difference.
        window: Duration
    },
    /// Lower bound given by an event of another predicate.
    Between {
        /// Index of the other predicate.
        lower: usize
    },
}

/// Time constraints of an event predicate.
#[derive(Clone, Debug)]
pub struct Timing {
    /// Index of a predicate whose associated event serves as upper bound.
    pub upper: usize,
    /// Lower bound.
    pub bound: TimingBound,
}

/// Direction of the sorting.
#[derive(Clone, Debug)]
pub enum Order {
    /// Ascendant sorting.
    Asc,
    /// Descendant sorting.
    Desc,
}

/// Static tuples ordering.
#[derive(Clone, Debug)]
pub struct Ordering {
    /// Index of the attribute to use for the sorting.
    pub attribute: usize,
    /// Direction of the sorting.
    pub direction: Order,
}

/// Predicate types.
#[derive(Clone, Debug)]
pub enum PredicateType {
    /// Predicate that trigger the rule evaluation.
    Trigger {
        /// Parameters associated to the trigger.
        parameters: Vec<ParameterDeclaration>,
    },
    /// Event predicate.
    Event {
        /// Selection policy.
        selection: EventSelection,
        /// Parameters associated to the predicate.
        parameters: Vec<ParameterDeclaration>,
        /// Time constraints for the predicate
        timing: Timing,
    },
    /// Static predicate that require a data ordering.
    ///
    /// # Note
    ///
    /// The selection policy is always `FIRST`,
    /// since you can always specify the opposite ordering direction,
    /// therefore it is omitted.
    OrderedStatic {
        /// Parameters associated to the predicate.
        parameters: Vec<ParameterDeclaration>,
        /// Ordering of the tuples.
        ordering: Vec<Ordering>,
    },
    /// Static predicate that does not require an ordering.
    ///
    /// # Note
    ///
    /// There is only one selection policy `EACH`, therefore it is omitted.
    UnorderedStatic {
        /// Parameters associated to the predicate.
        parameters: Vec<ParameterDeclaration>,
    },
    /// Event aggregate predicate.
    EventAggregate {
        /// Aggregate function.
        aggregator: Aggregator,
        /// Parameter representing the aggregate result.
        parameter: ParameterDeclaration,
        /// Time window constraints.
        timing: Timing,
    },
    /// Static aggregate predicate.
    StaticAggregate {
        /// Aggregate function.
        aggregator: Aggregator,
        /// Parameter representing the aggregate result.
        parameter: ParameterDeclaration,
    },
    /// Event absence predicate.
    EventNegation {
        /// Time window constraints.
        timing: Timing
    },
    /// Static non-existence predicate.
    StaticNegation,
}

/// Generic tuple constraints.
#[derive(Clone, Debug)]
pub struct ConstrainedTuple {
    /// Tuple declaration id.
    pub ty_id: usize,
    /// Boolean expressions.
    pub constraints: Vec<Expression>,
    /// Tuple alias.
    pub alias: String,
}

/// Rule predicate.
#[derive(Clone, Debug)]
pub struct Predicate {
    /// Type of predicate.
    pub ty: PredicateType,
    /// Constrained tuple.
    pub tuple: ConstrainedTuple,
}

impl PredicateType {
    /// Extracts the parameters used in the parameter definition..
    fn get_used_parameters(&self) -> Vec<(usize, usize)> {
        match *self {
            PredicateType::Trigger { ref parameters } |
            PredicateType::Event { ref parameters, .. } |
            PredicateType::OrderedStatic { ref parameters, .. } |
            PredicateType::UnorderedStatic { ref parameters } => {
                let mut res = parameters.iter()
                    .flat_map(|it| it.expression.get_parameters())
                    .collect::<Vec<_>>();
                res.sort();
                res.dedup();
                res
            }
            PredicateType::EventAggregate { ref parameter, .. } |
            PredicateType::StaticAggregate { ref parameter, .. } => {
                parameter.expression.get_parameters()
            }
            _ => Vec::new(),
        }
    }
}

impl ConstrainedTuple {
    /// Extracts the parameters used in the constraints.
    fn get_used_parameters(&self) -> Vec<(usize, usize)> {
        let mut res = self.constraints
            .iter()
            .flat_map(|it| it.get_parameters())
            .collect::<Vec<_>>();
        res.sort();
        res.dedup();
        res
    }
}

impl Predicate {
    /// Extracts the parameters used in the predicate.
    pub fn get_used_parameters(&self) -> Vec<(usize, usize)> {
        let mut res = self.ty.get_used_parameters();
        res.append(&mut self.tuple.get_used_parameters());
        res.sort();
        res.dedup();
        res
    }
}
