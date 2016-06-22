use tesla::{Event, Rule, Tuple, TupleDeclaration};
use tesla::expressions::*;
use tesla::predicates::*;
use std::rc::Rc;
use std::cmp::Ordering as CmpOrd;
use std::collections::{BTreeMap, HashMap};
use std::iter::{FromIterator, IntoIterator, once};
use chrono::{DateTime, Duration, UTC};
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
    fn evaluate(&self, context: &CompleteContext) -> Vec<PartialResult>;
}

#[derive(Clone, Debug)]
struct Trigger {
    predicate: Predicate,
}

impl Trigger {
    pub fn new(predicate: &Predicate) -> Self {
        Trigger { predicate: predicate.clone() }
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
struct Stack {
    tuple: TupleDeclaration,
    predicate: Predicate,
    local_exprs: Vec<Rc<Expression>>,
    global_exprs: Vec<Rc<Expression>>,
    timing: Timing,
    events: Vec<Rc<Event>>,
}

impl Stack {
    fn new(tuple: &TupleDeclaration, predicate: &Predicate) -> Option<Stack> {
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
    fn evaluate(&self, context: &CompleteContext) -> Vec<PartialResult> {
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

        // TODO maybe improve time bounds with binary search
        let mut iterator = self.events.iter();

        match self.predicate.ty {
            PredicateType::Event { ref selection, .. } => {
                let map_res = |evt: &Rc<Event>| context.get_result().clone().push_event(evt);
                match *selection {
                    EventSelection::Each => iterator.filter(check_evt).map(map_res).collect(),
                    EventSelection::First => {
                        iterator.find(check_evt).map(map_res).into_iter().collect()
                    }
                    EventSelection::Last => {
                        iterator.rev().find(check_evt).map(map_res).into_iter().collect()
                    }
                }
            }
            PredicateType::EventAggregate { ref aggregator, .. } => {
                let map_res = |res: Value| context.get_result().clone().push_aggregate(res);
                compute_aggregate(aggregator,
                                  iterator.filter(check_evt),
                                  &self.tuple.attributes)
                    .map(map_res)
                    .into_iter()
                    .collect()
            }
            PredicateType::EventNegation { .. } => {
                if !iterator.any(|evt| check_evt(&evt)) {
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
                    Stack::new(&declarations[&pred.tuple.ty_id], pred).map(|stack| (i, stack))
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

    fn get_partial_results(&self, event: &Rc<Event>) -> Vec<PartialResult> {
        let initial = PartialResult::with_trigger(event);
        self.stacks
            .iter()
            .fold(vec![initial], |previous, (_, stack)| {
                previous.iter()
                    .flat_map(|res| {
                        stack.evaluate(&CompleteContext::new(self.rule.predicates(), res))
                    })
                    .collect()
                // TODO maybe interrupt fold if prev is empty (combo scan + take_while + last)
            })
    }

    fn generate_events(&self, event: &Rc<Event>, results: &[PartialResult]) -> Vec<Rc<Event>> {
        results.iter()
            .map(|res| {
                let context = CompleteContext::new(self.rule.predicates(), res);
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

        if self.trigger.is_satisfied(event) {
            self.remove_old_events(&event.time);
            // TODO maybe work directly with contexts instead of partial results
            let partial_results = self.get_partial_results(event);
            // TODO filter for where clause
            // TODO consuming clause
            self.generate_events(event, &partial_results)
        } else {
            Vec::new()
        }
    }
}
