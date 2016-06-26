use tesla::*;
use tesla::expressions::*;
use chrono::{DateTime, UTC};
use linear_map::LinearMap;
use std::sync::Arc;
use trex::operations::*;

impl Value {
    fn cast(&self, ty: &BasicType) -> Value {
        match (ty, self) {
            (&BasicType::Float, &Value::Int(val)) => Value::Float(val as f32),
            _ => panic!("Wrong casting"),
        }
    }
    pub fn as_bool(&self) -> Option<bool> {
        if let Value::Bool(value) = *self { Some(value) } else { None }
    }
}

#[derive(Clone, Debug)]
pub struct PartialResult {
    parameters: LinearMap<(usize, usize), Value>,
    events: LinearMap<usize, Arc<Event>>,
}

impl PartialResult {
    pub fn new() -> Self {
        PartialResult {
            parameters: LinearMap::new(),
            events: LinearMap::new(),
        }
    }

    #[inline(always)]
    pub fn insert_event(mut self, idx: usize, event: Arc<Event>) -> Self {
        self.events.insert(idx, event);
        self
    }

    #[inline(always)]
    pub fn insert_parameter(mut self, idx: (usize, usize), parameter: Value) -> Self {
        self.parameters.insert(idx, parameter);
        self
    }

    #[inline(always)]
    pub fn get_time(&self, index: usize) -> DateTime<UTC> {
        self.events[&index].time
    }
}

pub trait EvaluationContext {
    fn get_attribute(&self, attribute: usize) -> Value;

    fn get_aggregate(&self) -> Value;

    fn get_parameter(&self, predicate: usize, parameter: usize) -> Value;

    fn evaluate_expression(&self, expression: &Expression) -> Value {
        match *expression {
            Expression::Immediate { ref value } => value.clone(),
            Expression::Reference { attribute } => self.get_attribute(attribute),
            Expression::Aggregate => self.get_aggregate(),
            Expression::Parameter { predicate, parameter } => {
                self.get_parameter(predicate, parameter)
            }
            Expression::Cast { ref ty, ref expression } => {
                self.evaluate_expression(expression).cast(ty)
            }
            Expression::UnaryOperation { ref operator, ref expression } => {
                unary::apply(operator, &self.evaluate_expression(expression))
            }
            Expression::BinaryOperation { ref operator, ref left, ref right } => {
                binary::apply(operator,
                              &self.evaluate_expression(left),
                              &self.evaluate_expression(right))
            }
        }
    }
}

#[derive(Clone, Debug)]
pub struct SimpleContext<'a> {
    tuple: &'a Tuple,
}

impl<'a> SimpleContext<'a> {
    pub fn new(tuple: &'a Tuple) -> Self {
        SimpleContext { tuple: tuple }
    }
}

impl<'a> EvaluationContext for SimpleContext<'a> {
    fn get_attribute(&self, attribute: usize) -> Value {
        self.tuple.data[attribute].clone()
    }

    fn get_aggregate(&self) -> Value {
        panic!("SimpleContext cannot retrieve aggregates");
    }

    fn get_parameter(&self, _: usize, _: usize) -> Value {
        panic!("SimpleContext cannot retrieve parameters");
    }
}

#[derive(Clone, Debug)]
pub enum CurrentValue<'a> {
    Empty,
    Aggr(&'a Value),
    Tuple(&'a Tuple),
}

impl<'a> From<()> for CurrentValue<'a> {
    fn from(_: ()) -> Self {
        CurrentValue::Empty
    }
}

impl<'a> From<&'a Value> for CurrentValue<'a> {
    fn from(aggr: &'a Value) -> Self {
        CurrentValue::Aggr(aggr)
    }
}

impl<'a> From<&'a Tuple> for CurrentValue<'a> {
    fn from(tuple: &'a Tuple) -> Self {
        CurrentValue::Tuple(tuple)
    }
}

#[derive(Clone, Debug)]
pub struct CompleteContext<'a> {
    result: &'a PartialResult,
    current: CurrentValue<'a>,
}

impl<'a> CompleteContext<'a> {
    pub fn new<T>(result: &'a PartialResult, current: T) -> Self
        where T: Into<CurrentValue<'a>>
    {
        CompleteContext {
            result: result,
            current: current.into(),
        }
    }
}

impl<'a> EvaluationContext for CompleteContext<'a> {
    fn get_attribute(&self, attribute: usize) -> Value {
        if let CurrentValue::Tuple(tuple) = self.current {
            tuple.data[attribute].clone()
        } else {
            panic!("Trying to get a tuple attribute on an aggregate")
        }
    }

    fn get_aggregate(&self) -> Value {
        if let CurrentValue::Aggr(aggr) = self.current {
            aggr.clone()
        } else {
            panic!("Trying to get an aggregate attribute on a tuple")
        }
    }

    fn get_parameter(&self, predicate: usize, parameter: usize) -> Value {
        self.result.parameters[&(predicate, parameter)].clone()
    }
}
