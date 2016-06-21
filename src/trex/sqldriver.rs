use tesla::*;
use tesla::expressions::*;
use tesla::predicates::*;

pub trait ToSQL {
    fn to_sql(&self, tuple: &TupleDeclaration) -> String;
}

impl ToSQL for Value {
    fn to_sql(&self, _: &TupleDeclaration) -> String {
        match self {
            &Value::Int(value) => format!("{}", value),
            &Value::Float(value) => format!("{}", value),
            &Value::Bool(value) => format!("{}", value),
            &Value::Str(ref value) => format!("{:?}", value),
        }
    }
}

impl ToSQL for UnaryOperator {
    fn to_sql(&self, _: &TupleDeclaration) -> String {
        match self {
                &UnaryOperator::Minus => "-",
                &UnaryOperator::Not => "!",
            }
            .to_owned()
    }
}

impl ToSQL for BinaryOperator {
    fn to_sql(&self, _: &TupleDeclaration) -> String {
        match self {
                &BinaryOperator::Plus => "+",
                &BinaryOperator::Minus => "-",
                &BinaryOperator::Times => "*",
                &BinaryOperator::Division => "/",
                &BinaryOperator::Equal => "=",
                &BinaryOperator::NotEqual => "!=",
                &BinaryOperator::GreaterThan => ">",
                &BinaryOperator::GreaterEqual => ">=",
                &BinaryOperator::LowerThan => "<",
                &BinaryOperator::LowerEqual => "<=",
            }
            .to_owned()
    }
}

impl ToSQL for Expression {
    fn to_sql(&self, tuple: &TupleDeclaration) -> String {
        match self {
            &Expression::Immediate { ref value } => value.to_sql(tuple),
            &Expression::Reference { attribute } => {
                format!("{}.{}", tuple.name, tuple.attributes[attribute].name)
            }
            &Expression::Parameter { predicate, parameter } => {
                format!(":param{}x{}", predicate, parameter)
            }
            &Expression::Aggregate => "trexaggregate".to_owned(),
            &Expression::Cast { ref expression, .. } => expression.to_sql(tuple),
            &Expression::UnaryOperation { ref operator, ref expression } => {
                format!("({}{})", operator.to_sql(tuple), expression.to_sql(tuple))
            }
            &Expression::BinaryOperation { ref operator, ref left, ref right } => {
                format!("({} {} {})",
                        left.to_sql(tuple),
                        operator.to_sql(tuple),
                        right.to_sql(tuple))
            }
        }
    }
}

impl ToSQL for ParameterDeclaration {
    fn to_sql(&self, tuple: &TupleDeclaration) -> String {
        format!("{} AS {}", self.expression.to_sql(tuple), self.name)
    }
}

impl ToSQL for Order {
    fn to_sql(&self, _: &TupleDeclaration) -> String {
        match *self {
                Order::Asc => "ASC",
                Order::Desc => "DESC",
            }
            .to_owned()
    }
}

impl ToSQL for Ordering {
    fn to_sql(&self, tuple: &TupleDeclaration) -> String {
        format!("{}.{} {}",
                tuple.name,
                tuple.attributes[self.attribute].name,
                self.direction.to_sql(tuple))
    }
}

impl ToSQL for Aggregator {
    fn to_sql(&self, tuple: &TupleDeclaration) -> String {
        match self {
            &Aggregator::Avg(attribute) => {
                format!("AVG({}.{})", tuple.name, tuple.attributes[attribute].name)
            }
            &Aggregator::Sum(attribute) => {
                format!("SUM({}.{})", tuple.name, tuple.attributes[attribute].name)
            }
            &Aggregator::Max(attribute) => {
                format!("MAX({}.{})", tuple.name, tuple.attributes[attribute].name)
            }
            &Aggregator::Min(attribute) => {
                format!("MIN({}.{})", tuple.name, tuple.attributes[attribute].name)
            }
            &Aggregator::Count => "COUNT(*)".to_owned(),
        }
    }
}

impl ToSQL for Predicate {
    fn to_sql(&self, tuple: &TupleDeclaration) -> String {
        let selection;
        let filters = self.tuple
            .constraints
            .iter()
            .map(|expr| expr.to_sql(tuple))
            .collect::<Vec<_>>()
            .join(" AND ");
        let mut rest = String::new();

        match &self.ty {
            &PredicateType::OrderdStatic { ref parameters, ref ordering } => {
                selection = parameters.iter()
                    .map(|par| par.to_sql(tuple))
                    .collect::<Vec<_>>()
                    .join(", ");
                let order_by = ordering.iter()
                    .map(|ord| ord.to_sql(tuple))
                    .collect::<Vec<_>>()
                    .join(", ");
                rest = format!("ORDER BY {} LIMIT 1", order_by);
            }
            &PredicateType::UnorderedStatic { ref parameters } => {
                selection = parameters.iter()
                    .map(|par| par.to_sql(tuple))
                    .collect::<Vec<_>>()
                    .join(", ");
            }
            &PredicateType::StaticAggregate { ref aggregator, ref parameter } => {
                selection = format!("{} AS trexaggregate, {}",
                                    aggregator.to_sql(tuple),
                                    parameter.to_sql(tuple));
            }
            &PredicateType::StaticNegation => {
                selection = "1".to_owned();
                rest = "LIMIT 1".to_owned();
            }
            _ => panic!("Error composing the SQL statement"),
        }

        format!("SELECT {} FROM {} WHERE {} {}",
                selection,
                tuple.name,
                filters,
                rest)
    }
}
