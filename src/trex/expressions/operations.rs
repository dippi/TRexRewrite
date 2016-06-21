pub mod unary {
    use tesla::expressions::*;

    fn minus(value: &Value) -> Value {
        match value {
            &Value::Int(x) => Value::Int(-x),
            &Value::Float(x) => Value::Float(-x),
            _ => panic!("Wrong use of unary minus"),
        }
    }

    fn not(value: &Value) -> Value {
        match value {
            &Value::Bool(x) => Value::Bool(!x),
            _ => panic!("Wrong use of not operator"),
        }
    }

    pub fn apply(operator: &UnaryOperator, value: &Value) -> Value {
        match operator {
            &UnaryOperator::Minus => minus(value),
            &UnaryOperator::Not => not(value),
        }
    }
}

pub mod binary {
    use tesla::expressions::*;

    fn plus(left: &Value, right: &Value) -> Value {
        match (left, right) {
            (&Value::Int(lhs), &Value::Int(rhs)) => Value::Int(lhs + rhs),
            (&Value::Float(lhs), &Value::Float(rhs)) => Value::Float(lhs + rhs),
            (&Value::Str(ref lhs), &Value::Str(ref rhs)) => Value::Str(format!("{}{}", lhs, rhs)),
            _ => panic!("Wrong use of plus operator"),
        }
    }

    fn minus(left: &Value, right: &Value) -> Value {
        match (left, right) {
            (&Value::Int(lhs), &Value::Int(rhs)) => Value::Int(lhs - rhs),
            (&Value::Float(lhs), &Value::Float(rhs)) => Value::Float(lhs - rhs),
            _ => panic!("Wrong use of minus operator"),
        }
    }

    fn times(left: &Value, right: &Value) -> Value {
        match (left, right) {
            (&Value::Int(lhs), &Value::Int(rhs)) => Value::Int(lhs * rhs),
            (&Value::Float(lhs), &Value::Float(rhs)) => Value::Float(lhs * rhs),
            _ => panic!("Wrong use of times operator"),
        }
    }

    fn division(left: &Value, right: &Value) -> Value {
        match (left, right) {
            (&Value::Int(lhs), &Value::Int(rhs)) => Value::Int(lhs / rhs),
            (&Value::Float(lhs), &Value::Float(rhs)) => Value::Float(lhs / rhs),
            _ => panic!("Wrong use of division operator"),
        }
    }

    fn equal(left: &Value, right: &Value) -> Value {
        match (left, right) {
            (&Value::Int(lhs), &Value::Int(rhs)) => Value::Bool(lhs == rhs),
            (&Value::Float(lhs), &Value::Float(rhs)) => Value::Bool(lhs == rhs),
            (&Value::Bool(lhs), &Value::Bool(rhs)) => Value::Bool(lhs == rhs),
            (&Value::Str(ref lhs), &Value::Str(ref rhs)) => Value::Bool(lhs == rhs),
            _ => panic!("Wrong use of equal operator"),
        }
    }

    fn not_equal(left: &Value, right: &Value) -> Value {
        match (left, right) {
            (&Value::Int(lhs), &Value::Int(rhs)) => Value::Bool(lhs != rhs),
            (&Value::Float(lhs), &Value::Float(rhs)) => Value::Bool(lhs != rhs),
            (&Value::Bool(lhs), &Value::Bool(rhs)) => Value::Bool(lhs != rhs),
            (&Value::Str(ref lhs), &Value::Str(ref rhs)) => Value::Bool(lhs != rhs),
            _ => panic!("Wrong use of not_equal operator"),
        }
    }

    fn greater_than(left: &Value, right: &Value) -> Value {
        match (left, right) {
            (&Value::Int(lhs), &Value::Int(rhs)) => Value::Bool(lhs > rhs),
            (&Value::Float(lhs), &Value::Float(rhs)) => Value::Bool(lhs > rhs),
            (&Value::Str(ref lhs), &Value::Str(ref rhs)) => Value::Bool(lhs > rhs),
            _ => panic!("Wrong use of greater_than operator"),
        }
    }

    fn greater_equal(left: &Value, right: &Value) -> Value {
        match (left, right) {
            (&Value::Int(lhs), &Value::Int(rhs)) => Value::Bool(lhs >= rhs),
            (&Value::Float(lhs), &Value::Float(rhs)) => Value::Bool(lhs >= rhs),
            (&Value::Str(ref lhs), &Value::Str(ref rhs)) => Value::Bool(lhs >= rhs),
            _ => panic!("Wrong use of greater_equal operator"),
        }
    }

    fn lower_than(left: &Value, right: &Value) -> Value {
        match (left, right) {
            (&Value::Int(lhs), &Value::Int(rhs)) => Value::Bool(lhs < rhs),
            (&Value::Float(lhs), &Value::Float(rhs)) => Value::Bool(lhs < rhs),
            (&Value::Str(ref lhs), &Value::Str(ref rhs)) => Value::Bool(lhs < rhs),
            _ => panic!("Wrong use of lower_than operator"),
        }
    }

    fn lower_equal(left: &Value, right: &Value) -> Value {
        match (left, right) {
            (&Value::Int(lhs), &Value::Int(rhs)) => Value::Bool(lhs <= rhs),
            (&Value::Float(lhs), &Value::Float(rhs)) => Value::Bool(lhs <= rhs),
            (&Value::Str(ref lhs), &Value::Str(ref rhs)) => Value::Bool(lhs <= rhs),
            _ => panic!("Wrong use of lower_equal operator"),
        }
    }

    pub fn apply(operator: &BinaryOperator, left: &Value, right: &Value) -> Value {
        match operator {
            &BinaryOperator::Plus => plus(left, right),
            &BinaryOperator::Minus => minus(left, right),
            &BinaryOperator::Times => times(left, right),
            &BinaryOperator::Division => division(left, right),
            &BinaryOperator::Equal => equal(left, right),
            &BinaryOperator::NotEqual => not_equal(left, right),
            &BinaryOperator::GreaterThan => greater_than(left, right),
            &BinaryOperator::GreaterEqual => greater_equal(left, right),
            &BinaryOperator::LowerThan => lower_than(left, right),
            &BinaryOperator::LowerEqual => lower_equal(left, right),
        }
    }
}
