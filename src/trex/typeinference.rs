use std::sync::Arc;
use linear_map::LinearMap;
use tesla::expressions::*;
use tesla::predicates::*;
use tesla::{EventTemplate, Rule, TupleDeclaration, TupleType};
use trex::FnvHashMap;

// TODO improve error handling and more informative failure,
// or switch completely to a panic!() approach and defer checks to parser

#[derive(Clone, Debug)]
enum CurrentType<'a> {
    Empty,
    Aggr(BasicType),
    Tuple(&'a TupleDeclaration),
}

#[derive(Clone, Debug)]
struct InferenceContext<'a> {
    params: LinearMap<(usize, usize), BasicType>,
    current: CurrentType<'a>,
}

impl<'a> InferenceContext<'a> {
    fn new() -> Self {
        InferenceContext {
            params: LinearMap::new(),
            current: CurrentType::Empty,
        }
    }

    fn add_parameter(mut self, idx: (usize, usize), ty: BasicType) -> Self {
        self.params.insert(idx, ty);
        self
    }

    fn set_current(mut self, current: CurrentType<'a>) -> Self {
        self.current = current;
        self
    }

    fn reset_current(mut self) -> Self {
        self.current = CurrentType::Empty;
        self
    }

    fn get_params(self) -> LinearMap<(usize, usize), BasicType> {
        self.params
    }
}

impl<'a> InferenceContext<'a> {
    fn get_attribute_ty(&self, attribute: usize) -> Result<BasicType, String> {
        if let CurrentType::Tuple(tuple) = self.current {
            tuple.attributes
                .get(attribute)
                .map(|it| it.ty.clone())
                .ok_or("Attribute out of bound".to_owned())
        } else {
            Err("Cannot get attribute without a tuple".to_owned())
        }
    }

    fn get_aggregate_ty(&self) -> Result<BasicType, String> {
        if let CurrentType::Aggr(ref aggr) = self.current {
            Ok(aggr.clone())
        } else {
            Err("Cannot get aggregate".to_owned())
        }
    }

    fn get_parameter_ty(&self, predicate: usize, parameter: usize) -> Result<BasicType, String> {
        self.params
            .get(&(predicate, parameter))
            .cloned()
            .ok_or("No such parameter".to_owned())
    }

    fn infer_expression(&self, expression: &Expression) -> Result<BasicType, String> {
        match *expression {
            Expression::Immediate { ref value } => Ok(value.get_type()),
            Expression::Reference { attribute } => self.get_attribute_ty(attribute),
            Expression::Aggregate => self.get_aggregate_ty(),
            Expression::Parameter { predicate, parameter } => {
                self.get_parameter_ty(predicate, parameter)
            }
            Expression::Cast { ref ty, ref expression } => {
                self.infer_expression(expression).and_then(|inner| {
                    if *ty == BasicType::Float && inner == BasicType::Int {
                        Ok(BasicType::Float)
                    } else {
                        Err("Bad cast".to_owned())
                    }
                })
            }
            Expression::UnaryOperation { ref operator, ref expression } => {
                self.infer_expression(expression).and_then(|it| unary::get_type(operator, &it))
            }
            Expression::BinaryOperation { ref operator, ref left, ref right } => {
                self.infer_expression(left)
                    .and_then(|left| self.infer_expression(right).map(|right| (left, right)))
                    .and_then(|(left, right)| binary::get_type(operator, &left, &right))
            }
        }
    }
}

fn type_check_constraints<'a>(constraints: &'a [Arc<Expression>],
                              ctx: InferenceContext<'a>)
                              -> Result<InferenceContext<'a>, String> {
    constraints.iter().fold(Ok(ctx), |ctx, expr| {
        ctx.and_then(|ctx| {
            ctx.infer_expression(expr).and_then(|ty| {
                if let BasicType::Bool = ty {
                    Ok(ctx)
                } else {
                    Err("Non boolean contraint".to_owned())
                }
            })
        })
    })
}

fn type_check_predicate<'a>(i: usize,
                            pred: &'a Predicate,
                            tuples: &'a FnvHashMap<usize, TupleDeclaration>,
                            ctx: InferenceContext<'a>)
                            -> Result<InferenceContext<'a>, String> {
    tuples.get(&pred.tuple.ty_id)
        .ok_or("Predicate refers to unknown tuple".to_owned())
        .and_then(|tuple| {
            // TODO check that a static predicate refers to a static tuple
            match pred.ty {
                PredicateType::Trigger { ref parameters, .. } |
                PredicateType::Event { ref parameters, .. } |
                PredicateType::OrderdStatic { ref parameters, .. } |
                PredicateType::UnorderedStatic { ref parameters, .. } => {
                    parameters.iter()
                        .enumerate()
                        .fold(Ok(ctx.set_current(CurrentType::Tuple(tuple))),
                              |ctx, (j, param)| {
                                  ctx.and_then(|ctx| {
                                      ctx.infer_expression(&param.expression)
                                          .map(|ty| ctx.add_parameter((i, j), ty))
                                  })
                              })
                        .and_then(|ctx| type_check_constraints(&pred.tuple.constraints, ctx))
                }
                PredicateType::EventAggregate { ref aggregator, ref parameter, .. } |
                PredicateType::StaticAggregate { ref aggregator, ref parameter } => {
                    type_check_constraints(&pred.tuple.constraints,
                                           ctx.set_current(CurrentType::Tuple(tuple)))
                        .and_then(|ctx| {
                            aggregate::get_type(aggregator, tuple).and_then(|ty| {
                                let ctx = ctx.set_current(CurrentType::Aggr(ty));
                                ctx.infer_expression(&parameter.expression)
                                    .map(|ty| ctx.add_parameter((i, 0), ty))
                            })
                        })
                }
                PredicateType::EventNegation { .. } |
                PredicateType::StaticNegation => {
                    type_check_constraints(&pred.tuple.constraints,
                                           ctx.set_current(CurrentType::Tuple(tuple)))
                }
            }
        })
}

fn type_check_template<'a>(template: &'a EventTemplate,
                           tuples: &'a FnvHashMap<usize, TupleDeclaration>,
                           ctx: InferenceContext<'a>)
                           -> Result<InferenceContext<'a>, String> {
    tuples.get(&template.ty_id)
        .ok_or("The rule produce an unknown event".to_owned())
        .and_then(|tuple| {
            if let TupleType::Event = tuple.ty {
                if tuple.attributes.len() == template.attributes.len() {
                    template.attributes
                        .iter()
                        .zip(tuple.attributes.iter().map(|it| &it.ty))
                        .fold(Ok(ctx), |ctx, (expr, ty)| {
                            ctx.and_then(|ctx| {
                                ctx.infer_expression(expr)
                                    .and_then(|res| {
                                        if *ty == res {
                                            Ok(ctx)
                                        } else {
                                            Err("Wrong attribute assignment".to_owned())
                                        }
                                    })
                            })
                        })
                } else {
                    Err("Wrong number of attributes in event template".to_owned())
                }
            } else {
                Err("The rule produce a static tuple".to_owned())
            }
        })
}

// TODO think of a better name or maybe separate funtionality to get params types
pub fn check_rule(rule: &Rule,
                  tuples: &FnvHashMap<usize, TupleDeclaration>)
                  -> Result<LinearMap<(usize, usize), BasicType>, String> {
    rule.predicates
        .iter()
        .enumerate()
        .fold(Ok(InferenceContext::new()), |ctx, (i, pred)| {
            ctx.and_then(|ctx| type_check_predicate(i, pred, tuples, ctx.reset_current()))
        })
        .and_then(|ctx| type_check_constraints(&rule.filters, ctx.reset_current()))
        .and_then(|ctx| type_check_template(&rule.event_template, tuples, ctx.reset_current()))
        // TODO check consuming!
        .map(|ctx| ctx.get_params())
}

mod aggregate {
    use tesla::expressions::BasicType;
    use tesla::predicates::Aggregator;
    use tesla::TupleDeclaration;

    pub fn get_type(aggregator: &Aggregator,
                    tuple: &TupleDeclaration)
                    -> Result<BasicType, String> {
        match *aggregator {
                Aggregator::Avg(i) => {
                    match tuple.attributes[i].ty {
                        BasicType::Int | BasicType::Float => Some(BasicType::Float),
                        _ => None,
                    }
                }
                Aggregator::Sum(i) |
                Aggregator::Min(i) |
                Aggregator::Max(i) => {
                    match tuple.attributes[i].ty {
                        ref ty @ BasicType::Int |
                        ref ty @ BasicType::Float => Some(ty.clone()),
                        _ => None,
                    }
                }
                Aggregator::Count => Some(BasicType::Int),
            }
            .ok_or("Wrong attribute type in aggregate computation".to_owned())
    }
}

mod unary {
    use tesla::expressions::{BasicType, UnaryOperator};

    pub fn get_type(operator: &UnaryOperator, ty: &BasicType) -> Result<BasicType, String> {
        match *operator {
                UnaryOperator::Minus => {
                    match *ty {
                        BasicType::Int | BasicType::Float => Some(ty.clone()),
                        _ => None,
                    }
                }
                UnaryOperator::Not => {
                    if let BasicType::Bool = *ty { Some(ty.clone()) } else { None }
                }
            }
            .ok_or("Wrong operand type in unary operation".to_owned())
    }
}

mod binary {
    use tesla::expressions::{BasicType, BinaryOperator};

    pub fn get_type(operator: &BinaryOperator,
                    left: &BasicType,
                    right: &BasicType)
                    -> Result<BasicType, String> {
        match *operator {
                BinaryOperator::Plus | BinaryOperator::Minus | BinaryOperator::Times |
                BinaryOperator::Division => {
                    match (left, right) {
                        (&BasicType::Int, &BasicType::Int) => Some(BasicType::Int),
                        (&BasicType::Float, &BasicType::Float) => Some(BasicType::Float),
                        _ => None,
                    }
                }
                BinaryOperator::Equal | BinaryOperator::NotEqual => {
                    if left == right { Some(BasicType::Bool) } else { None }
                }
                BinaryOperator::GreaterThan |
                BinaryOperator::GreaterEqual |
                BinaryOperator::LowerThan |
                BinaryOperator::LowerEqual => {
                    match (left, right) {
                        (&BasicType::Int, &BasicType::Int) |
                        (&BasicType::Float, &BasicType::Float) |
                        (&BasicType::Str, &BasicType::Str) => Some(BasicType::Bool),
                        _ => None,
                    }
                }
            }
            .ok_or("Wrong operands type in binary operation".to_owned())
    }
}
