use tesla::{Event, Rule, TupleDeclaration};
use tesla::expressions::*;
use tesla::predicates::*;
use std::f32;
use std::rc::Rc;
use std::collections::{BTreeMap, HashMap};
use std::iter::{FromIterator, IntoIterator};
use std::ops::Add;
use chrono::{DateTime, Duration, UTC};
use trex::expressions::*;

fn ptr_eq<T>(a: *const T, b: *const T) -> bool {
    a == b
}

trait EventProcessor {
    fn process(&mut self, event: &Rc<Event>);
    fn consume(&mut self, event: &Rc<Event>);
}

trait Evaluator<'a> {
    fn evaluate(&'a self, context: &'a CompleteContext<'a>) -> Vec<PartialResult<'a>>;
}

#[derive(Clone, Debug)]
struct Trigger<'a> {
    predicate: &'a Predicate,
}

impl<'a> Trigger<'a> {
    pub fn new(predicate: &'a Predicate) -> Self {
        Trigger { predicate: predicate }
    }

    pub fn is_satisfied(&self, event: &Rc<Event>) -> bool {
        event.tuple.ty_id == self.predicate.tuple.ty_id &&
        {
            let context = SimpleContext::new(&event.tuple);
            let check_expr = |expr: &Rc<_>| context.evaluate_expression(expr).as_bool().unwrap();
            self.predicate.tuple.constraints.iter().all(check_expr)
        }
    }
}

#[derive(Clone, Debug)]
struct Stack<'a> {
    tuple: &'a TupleDeclaration,
    predicate: &'a Predicate,
    local_exprs: Vec<Rc<Expression>>,
    global_exprs: Vec<Rc<Expression>>,
    timing: &'a Timing,
    loopback: Vec<usize>,
    max_window: Duration,
    events: Vec<Rc<Event>>, // TODO shortcuts for dependencies and stuff
}

impl<'a> Stack<'a> {
    fn new(tuple: &'a TupleDeclaration, predicate: &'a Predicate) -> Option<Stack<'a>> {
        match predicate.ty {
            PredicateType::Event { ref timing, .. } |
            PredicateType::EventAggregate { ref timing, .. } |
            PredicateType::EventNegation { ref timing } => {
                let (local_exprs, global_exprs) = predicate.tuple
                    .constraints
                    .iter()
                    .cloned()
                    .partition(|expr| expr.is_local());

                Some(Stack {
                    tuple: tuple,
                    predicate: predicate,
                    local_exprs: local_exprs,
                    global_exprs: global_exprs,
                    timing: timing,
                    loopback: Vec::new(),
                    max_window: Duration::seconds(0),
                    events: Vec::new(),
                })
            }
            _ => None,
        }
    }

    fn is_locally_satisfied(&self, event: &Rc<Event>) -> bool {
        event.tuple.ty_id == self.predicate.tuple.ty_id &&
        {
            let context = SimpleContext::new(&event.tuple);
            let check_expr = |expr: &Rc<_>| context.evaluate_expression(expr).as_bool().unwrap();
            self.local_exprs.iter().all(check_expr)
        }
    }

    fn is_globally_satisfied<'b>(&self, context: &'b CompleteContext<'b>) -> bool {
        let check_expr = |expr: &Rc<_>| context.evaluate_expression(expr).as_bool().unwrap();
        self.global_exprs.iter().all(check_expr)
    }

    fn remove_old_events(&mut self, time: Option<DateTime<UTC>>) {
        // TODO implement
        unimplemented!();
    }

    fn compute_aggregate<'b, T>(&self, aggregator: &Aggregator, iterator: T) -> Option<Value>
        where T: Iterator<Item = &'b Rc<Event>>
    {
        match aggregator {
            &Aggregator::Avg(attr) => {
                match self.tuple.attributes[attr].ty {
                    BasicType::Int => {
                        let mapped = iterator.map(|evt| evt.tuple.data[attr].extract_int());
                        let (count, sum) = mapped.fold((0i32, 0), |acc, x| (acc.0 + 1, acc.1 + x));
                        if count > 0 { Some(Value::from(sum as f32 / count as f32)) } else { None }
                    }
                    BasicType::Float => {
                        let mapped = iterator.map(|evt| evt.tuple.data[attr].extract_float());
                        let (count, sum) =
                            mapped.fold((0i32, 0.0), |acc, x| (acc.0 + 1, acc.1 + x));
                        if count > 0 { Some(Value::from(sum / count as f32)) } else { None }
                    }
                    _ => panic!("Tring to compute aggregate on wrong Value type"),
                }
            }
            &Aggregator::Sum(attr) => {
                match self.tuple.attributes[attr].ty {
                    BasicType::Int => {
                        let mapped = iterator.map(|evt| evt.tuple.data[attr].extract_int());
                        Some(Value::from(mapped.fold(0, Add::add)))
                    }
                    BasicType::Float => {
                        let mapped = iterator.map(|evt| evt.tuple.data[attr].extract_float());
                        Some(Value::from(mapped.fold(0.0, Add::add)))
                    }
                    _ => panic!("Tring to compute aggregate on wrong Value type"),
                }
            }
            &Aggregator::Min(attr) => {
                match self.tuple.attributes[attr].ty {
                    BasicType::Int => {
                        let mapped = iterator.map(|evt| evt.tuple.data[attr].extract_int());
                        mapped.min().map(Value::from)
                    }
                    BasicType::Float => {
                        let mapped = iterator.map(|evt| evt.tuple.data[attr].extract_float());
                        let min = mapped.fold(f32::NAN, f32::min);
                        if !min.is_nan() { Some(Value::from(min)) } else { None }
                    }
                    _ => panic!("Tring to compute aggregate on wrong Value type"),
                }
            }
            &Aggregator::Max(attr) => {
                match self.tuple.attributes[attr].ty {
                    BasicType::Int => {
                        let mapped = iterator.map(|evt| evt.tuple.data[attr].extract_int());
                        mapped.max().map(Value::from)
                    }
                    BasicType::Float => {
                        let mapped = iterator.map(|evt| evt.tuple.data[attr].extract_float());
                        let max = mapped.fold(f32::NAN, f32::max);
                        if !max.is_nan() { Some(Value::from(max)) } else { None }
                    }
                    _ => panic!("Tring to compute aggregate on wrong Value type"),
                }
            }
            &Aggregator::Count => Some(Value::from(iterator.count() as i32)),
        }
    }
}

impl<'a> EventProcessor for Stack<'a> {
    fn process(&mut self, event: &Rc<Event>) {
        if self.is_locally_satisfied(event) {
            // TODO reason on precondition: all the events arrive in chronological order
            self.events.push(event.clone());
        }
    }

    fn consume(&mut self, event: &Rc<Event>) {
        self.events.retain(|evt| !ptr_eq(evt, event));
    }
}

impl<'a> Evaluator<'a> for Stack<'a> {
    fn evaluate(&'a self, context: &'a CompleteContext<'a>) -> Vec<PartialResult<'a>> {
        let result = context.get_result();
        let upper = result.get_time(self.timing.upper);
        let lower = match self.timing.bound {
            TimingBound::Within { window } => upper - window,
            TimingBound::Between { lower } => result.get_time(lower),
        };

        let check_evt = |evt: &&Rc<Event>| {
            // TODO think about interval (open vs close)
            evt.time < upper && evt.time >= lower &&
            self.is_globally_satisfied(&context.clone().set_tuple(&evt.tuple))
        };

        match self.predicate.ty {
            PredicateType::Event { ref selection, .. } => {
                let map_res = |evt: &'a Rc<Event>| context.get_result().clone().push_event(&evt);
                let mut iterator = self.events.iter();
                match selection {
                    &EventSelection::Each => iterator.filter(check_evt).map(map_res).collect(),
                    &EventSelection::First => {
                        iterator.rev().find(check_evt).map(map_res).into_iter().collect()
                    }
                    &EventSelection::Last => {
                        iterator.find(check_evt).map(map_res).into_iter().collect()
                    }
                }
            }
            PredicateType::EventAggregate { ref aggregator, .. } => {
                let iterator = self.events.iter().filter(check_evt);
                let map_res = |res: Value| context.get_result().clone().push_aggregate(res);
                Vec::from_iter(self.compute_aggregate(aggregator, iterator).map(map_res))
            }
            PredicateType::EventNegation { .. } => {
                if !self.events.iter().any(|evt| check_evt(&evt)) {
                    vec![context.get_result().clone()]
                } else {
                    Vec::new()
                }
            }
            _ => panic!("Wrong event stack evaluation"),
        }
    }
}

#[derive(Clone, Debug)]
struct RuleStacks<'a> {
    rule: &'a Rule,
    trigger: Trigger<'a>,
    stacks: BTreeMap<usize, Stack<'a>>,
}

impl<'a> RuleStacks<'a> {
    fn new(rule: &'a Rule, declarations: &'a HashMap<usize, TupleDeclaration>) -> Self {
        let predicates = rule.predicates();
        let trigger = Trigger::new(&predicates[0]);

        let mut stacks = predicates.iter()
            .enumerate()
            .filter_map(|(i, pred)| {
                Stack::new(&declarations[&pred.tuple.ty_id], pred).map(|stack| (i, stack))
            })
            .collect::<BTreeMap<usize, Stack<'a>>>();

        // TODO insert into the stack the missing info

        RuleStacks {
            rule: rule,
            trigger: trigger,
            stacks: stacks,
        }
    }

    fn process(&mut self, event: &Rc<Event>) {
        for (_, stack) in &mut self.stacks {
            stack.process(event);
        }
    }
}
