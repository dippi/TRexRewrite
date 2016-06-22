use std::vec::Vec;
use std::string::String;
use std::rc::Rc;
use tesla::expressions::Expression;
use chrono::Duration;

#[derive(Clone, Debug)]
pub enum EventSelection {
    Each,
    First,
    Last,
}

#[derive(Clone, Debug)]
pub enum Aggregator {
    Avg(usize),
    Sum(usize),
    Max(usize),
    Min(usize),
    Count, // TODO add ANY and ALL?
}

#[derive(Clone, Debug)]
pub struct ParameterDeclaration {
    pub name: String,
    pub expression: Rc<Expression>,
}

#[derive(Clone, Debug)]
pub enum TimingBound {
    Within {
        window: Duration,
    },
    Between {
        lower: usize,
    },
}

#[derive(Clone, Debug)]
pub struct Timing {
    pub upper: usize,
    pub bound: TimingBound,
}

#[derive(Clone, Debug)]
pub enum Order {
    Asc,
    Desc,
}

#[derive(Clone, Debug)]
pub struct Ordering {
    pub attribute: usize,
    pub direction: Order,
}

#[derive(Clone, Debug)]
pub enum PredicateType {
    Trigger {
        parameters: Vec<ParameterDeclaration>,
    },
    Event {
        selection: EventSelection,
        parameters: Vec<ParameterDeclaration>,
        timing: Timing,
    },
    OrderdStatic {
        // Selection mode always `FIRST`
        // (Last isn't sigificant since you can always specify the opposite ordering)
        parameters: Vec<ParameterDeclaration>,
        ordering: Vec<Ordering>,
    },
    UnorderedStatic {
        // Selection mode always `EACH`
        parameters: Vec<ParameterDeclaration>,
    },
    EventAggregate {
        aggregator: Aggregator,
        parameter: ParameterDeclaration,
        timing: Timing,
    },
    StaticAggregate {
        aggregator: Aggregator,
        parameter: ParameterDeclaration,
    },
    EventNegation {
        timing: Timing,
    },
    StaticNegation,
}

#[derive(Clone, Debug)]
pub struct ConstrainedTuple {
    pub ty_id: usize,
    pub constraints: Vec<Rc<Expression>>,
    pub alias: String,
}

#[derive(Clone, Debug)]
pub struct Predicate {
    pub ty: PredicateType,
    pub tuple: ConstrainedTuple,
}
