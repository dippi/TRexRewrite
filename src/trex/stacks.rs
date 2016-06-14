use tesla::{Event, Rule, Tuple, TupleDeclaration};
use tesla::expressions::*;
use tesla::predicates::*;
use std::rc::Rc;
use std::cmp::Ordering as CmpOrd;
use std::collections::{BTreeMap, HashMap};
use chrono::{DateTime, UTC};
use trex::expressions::*;
use trex::aggregators::compute_aggregate;

fn ptr_eq<T>(a: *const T, b: *const T) -> bool {
    a == b
}

pub trait EventProcessor {
    fn process(&mut self, event: &Rc<Event>);
    fn consume(&mut self, event: &Rc<Event>);
}

pub trait Evaluator {
    fn evaluate(&self, result: &PartialResult) -> Vec<PartialResult>;
}

#[derive(Clone, Debug)]
struct Trigger {
    predicate: Predicate,
}

impl Trigger {
    pub fn new(predicate: &Predicate) -> Self {
        Trigger { predicate: predicate.clone() }
    }

    pub fn is_satisfied(&self, context: &CompleteContext) -> bool {
        let check_expr = |expr: &Rc<_>| context.evaluate_expression(expr).as_bool().unwrap();
        self.predicate.tuple.constraints.iter().all(check_expr)
    }

    pub fn evaluate(&self, event: &Rc<Event>) -> Option<PartialResult> {
        if event.tuple.ty_id == self.predicate.tuple.ty_id {
            let res = if let PredicateType::Trigger { ref parameters } = self.predicate.ty {
                parameters.iter().enumerate().fold(PartialResult::new(), |res, (i, param)| {
                    let val = CompleteContext::new(&res, &event.tuple)
                        .evaluate_expression(&param.expression);
                    res.insert_parameter((0, i), val)
                })
            } else {
                panic!("Unexpected predicate type")
            };
            if self.is_satisfied(&CompleteContext::new(&res, &event.tuple)) {
                Some(res.insert_event(0, event.clone()))
            } else {
                None
            }
        } else {
            None
        }
    }
}

#[derive(Clone, Debug)]
struct Stack {
    idx: usize,
    tuple: TupleDeclaration,
    predicate: Predicate,
    local_exprs: Vec<Rc<Expression>>,
    global_exprs: Vec<Rc<Expression>>,
    timing: Timing,
    events: Vec<Rc<Event>>,
}

impl Stack {
    fn new(idx: usize, tuple: &TupleDeclaration, predicate: &Predicate) -> Option<Stack> {
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
                    idx: idx,
                    tuple: tuple.clone(),
                    predicate: predicate.clone(),
                    local_exprs: local_exprs,
                    global_exprs: global_exprs,
                    timing: timing.clone(),
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

    fn is_globally_satisfied(&self, context: &CompleteContext) -> bool {
        let check_expr = |expr: &Rc<_>| context.evaluate_expression(expr).as_bool().unwrap();
        self.global_exprs.iter().all(check_expr)
    }

    fn remove_old_events(&mut self,
                         times: &HashMap<usize, DateTime<UTC>>)
                         -> Option<DateTime<UTC>> {
        // TODO reason on interval (open vs close)
        let time = match self.timing.bound {
            TimingBound::Within { window } => times[&self.timing.upper] - window,
            TimingBound::Between { lower } => times[&lower],
        };

        let index = self.events
            .binary_search_by(|evt| {
                if evt.time < time { CmpOrd::Less } else { CmpOrd::Greater }
            })
            .unwrap_err();
        self.events.drain(..index);

        self.events.first().map(|evt| evt.time)
    }
}

impl EventProcessor for Stack {
    fn process(&mut self, event: &Rc<Event>) {
        if self.is_locally_satisfied(event) {
            // TODO reason on precondition: all the events arrive in chronological order
            self.events.push(event.clone());
        }
    }

    fn consume(&mut self, event: &Rc<Event>) {
        let index = {
            let start = self.events
                .binary_search_by(|evt| {
                    if evt.time < event.time { CmpOrd::Less } else { CmpOrd::Greater }
                })
                .unwrap_err();
            // TODO handle the absence of the event from the queue
            self.events[start..].iter().position(|evt| ptr_eq(evt, event)).unwrap() + start
        };
        self.events.remove(index);
    }
}

impl Evaluator for Stack {
    fn evaluate(&self, result: &PartialResult) -> Vec<PartialResult> {
        let upper_time = result.get_time(self.timing.upper);
        let lower_time = match self.timing.bound {
            TimingBound::Within { window } => upper_time - window,
            TimingBound::Between { lower } => result.get_time(lower),
        };

        let upper = self.events
            .binary_search_by(|evt| {
                if evt.time < upper_time { CmpOrd::Less } else { CmpOrd::Greater }
            })
            .unwrap_err();
        let lower = self.events
            .binary_search_by(|evt| {
                if evt.time < lower_time { CmpOrd::Less } else { CmpOrd::Greater }
            })
            .unwrap_err();

        let mut iterator = self.events[lower..upper].iter();

        match self.predicate.ty {
            PredicateType::Event { ref selection, ref parameters, .. } => {
                let filter_map = |evt: &Rc<Event>| {
                    let res =
                        parameters.iter().enumerate().fold(result.clone(), |res, (i, param)| {
                            let val = CompleteContext::new(&res, &evt.tuple)
                                .evaluate_expression(&param.expression);
                            res.insert_parameter((self.idx, i), val)
                        });
                    if self.is_globally_satisfied(&CompleteContext::new(&res, &evt.tuple)) {
                        Some(res.insert_event(self.idx, evt.clone()))
                    } else {
                        None
                    }
                };
                match *selection {
                    EventSelection::Each => iterator.filter_map(filter_map).collect(),
                    EventSelection::First => iterator.filter_map(filter_map).take(1).collect(),
                    EventSelection::Last => iterator.rev().filter_map(filter_map).take(1).collect(),
                }
            }
            PredicateType::EventAggregate { ref aggregator, ref parameter, .. } => {
                let check = |evt: &&Rc<Event>| {
                    self.is_globally_satisfied(&CompleteContext::new(result, &evt.tuple))
                };
                let map = |aggr: Value| {
                    let context = CompleteContext::new(result, &aggr);
                    let val = context.evaluate_expression(&parameter.expression);
                    result.clone().insert_parameter((self.idx, 0), val)
                };
                compute_aggregate(aggregator, iterator.filter(check), &self.tuple.attributes)
                    .map(map)
                    .into_iter()
                    .collect()
            }
            PredicateType::EventNegation { .. } => {
                let check = |evt: &Rc<Event>| {
                    self.is_globally_satisfied(&CompleteContext::new(&result, &evt.tuple))
                };
                if !iterator.any(check) { vec![result.clone()] } else { Vec::new() }
            }
            _ => panic!("Wrong event stack evaluation"),
        }
    }
}

#[derive(Clone, Debug)]
pub struct RuleStacks {
    rule: Rule,
    trigger: Trigger,
    stacks: BTreeMap<usize, Stack>,
}

impl RuleStacks {
    pub fn new(rule: Rule, declarations: &HashMap<usize, TupleDeclaration>) -> Self {
        let (trigger, stacks) = {
            let predicates = rule.predicates();
            let trigger = Trigger::new(&predicates[0]);

            let stacks = predicates.iter()
                .enumerate()
                .filter_map(|(i, pred)| {
                    Stack::new(i, &declarations[&pred.tuple.ty_id], pred).map(|stack| (i, stack))
                })
                .collect::<BTreeMap<usize, Stack>>();

            (trigger, stacks)
        };

        RuleStacks {
            rule: rule,
            trigger: trigger,
            stacks: stacks,
        }
    }

    fn remove_old_events(&mut self, trigger_time: &DateTime<UTC>) {
        let mut times = HashMap::new();
        times.insert(0, *trigger_time);
        for (&i, stack) in &mut self.stacks {
            let time = stack.remove_old_events(&times).unwrap_or(*trigger_time);
            times.insert(i, time);
        }
    }

    fn get_partial_results(&self, initial: PartialResult) -> Vec<PartialResult> {
        self.stacks
            .iter()
            .fold(vec![initial], |previous, (_, stack)| {
                previous.iter().flat_map(|res| stack.evaluate(res)).collect()
                // TODO maybe interrupt fold if prev is empty (combo scan + take_while + last)
            })
    }

    fn generate_events(&self, event: &Rc<Event>, results: &[PartialResult]) -> Vec<Rc<Event>> {
        results.iter()
            .map(|res| {
                let context = CompleteContext::new(res, ());
                let template = self.rule.event_template();
                Rc::new(Event {
                    tuple: Tuple {
                        ty_id: template.ty_id,
                        data: template.attributes
                            .iter()
                            .map(|expr| context.evaluate_expression(expr))
                            .collect(),
                    },
                    time: event.time,
                })
            })
            .collect()
    }

    pub fn process(&mut self, event: &Rc<Event>) -> Vec<Rc<Event>> {
        for (_, stack) in &mut self.stacks {
            stack.process(event);
        }

        if let Some(initial) = self.trigger.evaluate(event) {
            self.remove_old_events(&event.time);
            let partial_results = self.get_partial_results(initial);
            // TODO filter for where clause
            // TODO consuming clause
            self.generate_events(event, &partial_results)
        } else {
            Vec::new()
        }
    }
}
