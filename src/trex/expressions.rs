use tesla::*;
use tesla::expressions::*;
use tesla::predicates::*;
use chrono::{DateTime, UTC};
use owning_ref::{ErasedRcRef, RcRef};
use std::collections::HashMap;
use std::rc::Rc;
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

impl Expression {
    pub fn is_local(&self) -> bool {
        // TODO maybe take into account local parameters that don't alter expression locality
        match *self {
            Expression::Parameter { .. } => false,
            Expression::Cast { ref expression, .. } |
            Expression::UnaryOperation { ref expression, .. } => expression.is_local(),
            Expression::BinaryOperation { ref left, ref right, .. } => {
                left.is_local() && right.is_local()
            }
            _ => true,
        }
    }
}

#[derive(Clone, Debug)]
pub struct PartialResult {
    tuples: HashMap<usize, ErasedRcRef<Tuple>>,
    aggregates: HashMap<usize, Value>,
    times: HashMap<usize, DateTime<UTC>>,
    len: usize,
}

impl PartialResult {
    pub fn new() -> Self {
        PartialResult {
            tuples: HashMap::new(),
            aggregates: HashMap::new(),
            times: HashMap::new(),
            len: 0,
        }
    }

    pub fn with_trigger(trigger: &Rc<Event>) -> Self {
        PartialResult::new().push_event(trigger)
    }

    pub fn push_event(mut self, event: &Rc<Event>) -> Self {
        let tuple = RcRef::new(event.clone()).map(|evt| &evt.tuple).erase_owner();
        self.tuples.insert(self.len, tuple);
        self.times.insert(self.len, event.time);
        self.len += 1;
        self
    }

    pub fn push_tuple(mut self, tuple: &Rc<Tuple>) -> Self {
        self.tuples.insert(self.len, RcRef::new(tuple.clone()).erase_owner());
        self.len += 1;
        self
    }

    pub fn push_aggregate(mut self, aggregate: Value) -> Self {
        self.aggregates.insert(self.len, aggregate);
        self.len += 1;
        self
    }

    pub fn get_time(&self, index: usize) -> DateTime<UTC> {
        self.times[&index]
    }
}

pub trait EvaluationContext {
    fn get_attribute(&self, attribute: usize) -> Value;

    fn get_aggregate(&self) -> Value;

    fn evaluate_parameter(&self, predicate: usize, parameter: usize) -> Value;

    fn evaluate_expression(&self, expression: &Expression) -> Value {
        match *expression {
            Expression::Immediate { ref value } => value.clone(),
            Expression::Reference { attribute } => self.get_attribute(attribute),
            Expression::Aggregate => self.get_aggregate(),
            Expression::Parameter { predicate, parameter } => {
                self.evaluate_parameter(predicate, parameter)
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

    fn evaluate_parameter(&self, _: usize, _: usize) -> Value {
        panic!("SimpleContext cannot evaluate parameters");
    }
}

#[derive(Clone, Debug)]
pub struct CompleteContext<'a> {
    predicates: &'a [Predicate],
    result: &'a PartialResult,
    current: usize,
    tuple: Option<&'a Tuple>,
}

impl<'a> CompleteContext<'a> {
    pub fn new(predicates: &'a [Predicate], result: &'a PartialResult) -> Self {
        CompleteContext {
            predicates: predicates,
            result: result,
            current: 0,
            tuple: None,
        }
    }

    pub fn set_current(mut self, current: usize) -> Self {
        self.current = current;
        self.tuple = self.result.tuples.get(&current).map(|it| &**it);
        self
    }

    pub fn set_tuple(mut self, tuple: &'a Tuple) -> Self {
        self.tuple = Some(tuple);
        self.current += 1;
        self
    }

    pub fn get_result(&self) -> &PartialResult {
        self.result
    }
}

impl<'a> EvaluationContext for CompleteContext<'a> {
    fn get_attribute(&self, attribute: usize) -> Value {
        self.tuple.unwrap().data[attribute].clone()
    }

    fn get_aggregate(&self) -> Value {
        self.result.aggregates[&self.current].clone()
    }

    fn evaluate_parameter(&self, predicate: usize, parameter: usize) -> Value {
        let expression = match self.predicates[predicate].ty {
            PredicateType::Trigger { ref parameters } |
            PredicateType::Event { ref parameters, .. } |
            PredicateType::OrderdStatic { ref parameters, .. } |
            PredicateType::UnorderedStatic { ref parameters } => &parameters[parameter].expression,
            PredicateType::EventAggregate { ref parameter, .. } |
            PredicateType::StaticAggregate { ref parameter, .. } => &parameter.expression,
            _ => panic!("Wrong parameters evaluation"),
        };

        self.clone().set_current(predicate).evaluate_expression(expression)
    }
}
