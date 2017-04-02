//! TESLA algebraic and boolean expressions.
//!
//! This module contains the structures that represent the expressions AST,
//! composed of basic types, values and operations.

// TODO explain that TESLA is static typed, but dynamically interpreted

use ordered_float::NotNaN;
use std::hash::{Hash, Hasher};

/// The basic types of attributes, parameters, aggregates and immediate values.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum BasicType {
    /// 64 bytes signed integer
    Int,
    /// 64 bytes signed floating point
    Float,
    /// Boolean
    Bool,
    /// String
    Str,
}

/// The basic values of attributes, parameters, aggregates and immediate values.
#[derive(Clone, Debug)]
pub enum Value {
    /// 64 bytes signed integer.
    Int(i64),
    /// 64 bytes signed floating point.
    Float(f64),
    /// Boolean.
    Bool(bool),
    /// String.
    Str(String),
}

// TODO add a RawValue(i32, f32, bool, String)?
// Paying a little cost in memory It would allow unchecked access to values
// But for safety and ergonomy it could be easily converted to/from the Value enum.

/// Unary operators.
#[derive(Clone, Debug)]
pub enum UnaryOperator {
    /// Algebraic negation operator.
    Minus,
    /// Boolean negation operator.
    Not,
}

/// Binary operators.
#[derive(Clone, Debug)]
pub enum BinaryOperator {
    /// Sum operator.
    Plus,
    /// Subtraction operator.
    Minus,
    /// Multiplication operator.
    Times,
    /// Division operator.
    Division,
    /// Equality operator.
    Equal,
    /// Inequality operator.
    NotEqual,
    /// Greater than comparison operator.
    GreaterThan,
    /// Greater or equal comparison operator.
    GreaterEqual,
    /// Lower than comparison operator.
    LowerThan,
    /// Lower or equal comparison operator.
    LowerEqual,
    // TODO add Reminder operator
}

/// Expressions AST node.
#[derive(Clone, Debug)]
pub enum Expression {
    /// Immediate value.
    Immediate {
        /// The immediate value.
        value: Value
    },
    /// Reference to a tuple attribute value.
    ///
    /// It always refers to the tuple associated to the predicate it appears in.
    Reference {
        /// Attribute index in the tuple
        attribute: usize
    },
    /// Aggregate result value.
    ///
    /// It refers to the value of the aggregation predicate it appears in.
    Aggregate,
    /// Reference to a parameter value.
    Parameter {
        /// Index of the predicate where the parameter is defined.
        predicate: usize,
        /// Index of the parameter within the other predicate parameters.
        parameter: usize, // TODO maybe replace with Arc<Expression>
    },
    /// Cast between two compatible types
    Cast {
        /// The destination type.
        ty: BasicType,
        /// The expression whose result has to be casted.
        expression: Box<Expression>,
    },
    /// Unary operation node.
    UnaryOperation {
        /// Unary operator.
        operator: UnaryOperator,
        /// Expression subtree.
        expression: Box<Expression>,
    },
    /// Binary operation node.
    BinaryOperation {
        /// Binary operator.
        operator: BinaryOperator,
        /// Left subexpression.
        left: Box<Expression>,
        /// Right subexpression.
        right: Box<Expression>,
    },
}

// TODO think about utility of the following functions

impl Value {
    /// Unwraps an integer from a `Value` and panics if the type does not match.
    pub fn unwrap_int(&self) -> i64 {
        if let Value::Int(value) = *self { value } else { panic!("Wrong Value unwrap") }
    }
    /// Unwraps a Float from a `Value` and panics if the type does not match.
    pub fn unwrap_float(&self) -> f64 {
        if let Value::Float(value) = *self { value } else { panic!("Wrong Value unwrap") }
    }
    /// Unwraps a boolean from a `Value` and panics if the type does not match.
    pub fn unwrap_bool(&self) -> bool {
        if let Value::Bool(value) = *self { value } else { panic!("Wrong Value unwrap") }
    }
    /// Unwraps a string from a `Value` and panics if the type does not match.
    pub fn unwrap_string(&self) -> String {
        if let Value::Str(ref value) = *self { value.clone() } else { panic!("Wrong Value unwrap") }
    }
}

impl From<i64> for Value {
    fn from(val: i64) -> Self { Value::Int(val) }
}

impl From<f64> for Value {
    fn from(val: f64) -> Self { Value::Float(val) }
}

impl From<bool> for Value {
    fn from(val: bool) -> Self { Value::Bool(val) }
}

impl From<String> for Value {
    fn from(val: String) -> Self { Value::Str(val) }
}

impl Value {
    /// Returns the type of a value.
    pub fn get_type(&self) -> BasicType {
        match *self {
            Value::Int(_) => BasicType::Int,
            Value::Float(_) => BasicType::Float,
            Value::Bool(_) => BasicType::Bool,
            Value::Str(_) => BasicType::Str,
        }
    }
}

impl Hash for Value {
    fn hash<H: Hasher>(&self, state: &mut H) {
        match *self {
            Value::Int(x) => x.hash(state),
            Value::Float(x) => NotNaN::from(x).hash(state),
            Value::Bool(x) => x.hash(state),
            Value::Str(ref x) => x.hash(state),
        }
    }
}

impl PartialEq for Value {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (&Value::Int(x), &Value::Int(y)) => x.eq(&y),
            (&Value::Float(x), &Value::Float(y)) => NotNaN::from(x).eq(&NotNaN::from(y)),
            (&Value::Bool(x), &Value::Bool(y)) => x.eq(&y),
            (&Value::Str(ref x), &Value::Str(ref y)) => x.eq(y),
            _ => false,
        }
    }
}
impl Eq for Value {}

impl Expression {
    /// Checks if the expression does not have parameters.
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

    /// Extracts all the parameters used in the expression.
    pub fn get_parameters(&self) -> Vec<(usize, usize)> {
        match *self {
            Expression::Parameter { predicate, parameter } => vec![(predicate, parameter)],
            Expression::Cast { ref expression, .. } |
            Expression::UnaryOperation { ref expression, .. } => expression.get_parameters(),
            Expression::BinaryOperation { ref left, ref right, .. } => {
                let mut res = left.get_parameters();
                res.append(&mut right.get_parameters());
                res.sort();
                res.dedup();
                res
            }
            _ => Vec::new(),
        }
    }

    /// Returns the last predicate the expression depends on.
    pub fn get_last_predicate(&self) -> Option<usize> {
        self.get_parameters().last().map(|&(pred, _)| pred)
    }
}
