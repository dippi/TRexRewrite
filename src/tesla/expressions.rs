use std::rc::Rc;

#[derive(Clone, Debug)]
pub enum BasicType {
    Int,
    Float,
    Bool,
    Str,
}

#[derive(Clone, Debug)]
pub enum Value {
    Int(i32),
    Float(f32),
    Bool(bool),
    Str(String),
}

// TODO add a RawValue(i32, f32, bool, String)?
// Paying a little cost in memory It would allow unchecked access to values
// But for safety and ergonomy it could be easily converted to/from the Value enum.

#[derive(Clone, Debug)]
pub enum UnaryOperator {
    Minus,
    Not,
}

#[derive(Clone, Debug)]
pub enum BinaryOperator {
    Plus,
    Minus,
    Times,
    Division,
    Equal,
    NotEqual,
    GreaterThan,
    GreaterEqual,
    LowerThan,
    LowerEqual, // TODO add Reminder
}

#[derive(Clone, Debug)]
pub enum Expression {
    Immediate {
        value: Value,
    },
    /// It always refers to the predicate it appears in
    Reference {
        attribute: usize,
    },
    /// It refers to the value of the aggregation predicate it appears in
    Aggregate,
    Parameter {
        predicate: usize,
        parameter: usize, // TODO maybe replace with Rc<Expression>
    },
    Cast {
        ty: BasicType,
        expression: Rc<Expression>,
    },
    UnaryOperation {
        operator: UnaryOperator,
        expression: Rc<Expression>,
    },
    BinaryOperation {
        operator: BinaryOperator,
        left: Rc<Expression>,
        right: Rc<Expression>,
    },
}

// TODO think about utility of the following functions

impl Value {
    pub fn extract_int(&self) -> i32 {
        if let &Value::Int(value) = self { value } else { panic!("Wrong Value unwrap") }
    }
    pub fn extract_float(&self) -> f32 {
        if let &Value::Float(value) = self { value } else { panic!("Wrong Value unwrap") }
    }
    pub fn extract_bool(&self) -> bool {
        if let &Value::Bool(value) = self { value } else { panic!("Wrong Value unwrap") }
    }
    pub fn extract_string(&self) -> String {
        if let &Value::Str(ref value) = self { value.clone() } else { panic!("Wrong Value unwrap") }
    }
}

impl From<i32> for Value {
    fn from(val: i32) -> Self {
        Value::Int(val)
    }
}

impl From<f32> for Value {
    fn from(val: f32) -> Self {
        Value::Float(val)
    }
}

impl From<bool> for Value {
    fn from(val: bool) -> Self {
        Value::Bool(val)
    }
}

impl From<String> for Value {
    fn from(val: String) -> Self {
        Value::Str(val)
    }
}

impl From<Value> for Option<i32> {
    fn from(val: Value) -> Self {
        if let Value::Int(x) = val { Some(x) } else { None }
    }
}

impl From<Value> for Option<f32> {
    fn from(val: Value) -> Self {
        if let Value::Float(x) = val { Some(x) } else { None }
    }
}

impl From<Value> for Option<bool> {
    fn from(val: Value) -> Self {
        if let Value::Bool(x) = val { Some(x) } else { None }
    }
}

impl From<Value> for Option<String> {
    fn from(val: Value) -> Self {
        if let Value::Str(x) = val { Some(x) } else { None }
    }
}