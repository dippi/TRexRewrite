use linear_map::LinearMap;
use tesla::expressions::*;
use tesla::TupleDeclaration;

// TODO improve error handling and more informative failure,
// or switch completely to a panic!() approach and defer checks to parser

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

#[derive(Clone, Debug)]
pub enum CurrentType<'a> {
    Empty,
    Aggr(BasicType),
    Tuple(&'a TupleDeclaration),
}

#[derive(Clone, Debug)]
pub struct InferenceContext<'a> {
    params: LinearMap<(usize, usize), BasicType>,
    current: CurrentType<'a>,
}

impl<'a> InferenceContext<'a> {
    pub fn new() -> Self {
        InferenceContext {
            params: LinearMap::new(),
            current: CurrentType::Empty,
        }
    }

    pub fn add_parameter(mut self, idx: (usize, usize), ty: BasicType) -> Self {
        self.params.insert(idx, ty);
        self
    }

    pub fn set_current(mut self, current: CurrentType<'a>) -> Self {
        self.current = current;
        self
    }

    pub fn reset_current(mut self) -> Self {
        self.current = CurrentType::Empty;
        self
    }

    pub fn get_params(self) -> LinearMap<(usize, usize), BasicType> {
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

    pub fn infer_expression(&self, expression: &Expression) -> Result<BasicType, String> {
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
