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
    LowerEqual,
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
