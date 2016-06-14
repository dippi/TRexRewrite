use tesla::*;
use tesla::expressions::*;
use tesla::predicates::*;
use trex::stacks::*;
use trex::expressions::*;
use rusqlite::{Connection, Statement};
use r2d2::Pool;
use r2d2_sqlite::SqliteConnectionManager;

impl Value {
    fn get_sql(&self) -> String {
        match *self {
            Value::Int(value) => format!("{}", value),
            Value::Float(value) => format!("{}", value),
            Value::Bool(value) => format!("{}", value),
            // TODO check excaping for SQL injection
            Value::Str(ref value) => format!("{:?}", value),
        }
    }
}

impl UnaryOperator {
    fn get_sql(&self) -> String {
        match *self {
                UnaryOperator::Minus => "-",
                UnaryOperator::Not => "!",
            }
            .to_owned()
    }
}

impl BinaryOperator {
    fn get_sql(&self) -> String {
        match *self {
                BinaryOperator::Plus => "+",
                BinaryOperator::Minus => "-",
                BinaryOperator::Times => "*",
                BinaryOperator::Division => "/",
                BinaryOperator::Equal => "=",
                BinaryOperator::NotEqual => "!=",
                BinaryOperator::GreaterThan => ">",
                BinaryOperator::GreaterEqual => ">=",
                BinaryOperator::LowerThan => "<",
                BinaryOperator::LowerEqual => "<=",
            }
            .to_owned()
    }
}

impl Expression {
    fn get_sql(&self, tuple: &TupleDeclaration) -> String {
        match *self {
            Expression::Immediate { ref value } => value.get_sql(),
            Expression::Reference { attribute } => {
                format!("{}.{}", tuple.name, tuple.attributes[attribute].name)
            }
            Expression::Parameter { predicate, parameter } => {
                format!(":param{}x{}", predicate, parameter)
            }
            Expression::Aggregate => "trexaggregate".to_owned(),
            Expression::Cast { ref expression, .. } => expression.get_sql(tuple),
            Expression::UnaryOperation { ref operator, ref expression } => {
                format!("({}{})", operator.get_sql(), expression.get_sql(tuple))
            }
            Expression::BinaryOperation { ref operator, ref left, ref right } => {
                format!("({} {} {})",
                        left.get_sql(tuple),
                        operator.get_sql(),
                        right.get_sql(tuple))
            }
        }
    }
}

impl ParameterDeclaration {
    fn get_sql(&self, tuple: &TupleDeclaration) -> String {
        format!("{} AS {}", self.expression.get_sql(tuple), self.name)
    }
}

impl Order {
    fn get_sql(&self) -> String {
        match *self {
                Order::Asc => "ASC",
                Order::Desc => "DESC",
            }
            .to_owned()
    }
}

impl Ordering {
    fn get_sql(&self, tuple: &TupleDeclaration) -> String {
        format!("{}.{} {}",
                tuple.name,
                tuple.attributes[self.attribute].name,
                self.direction.get_sql())
    }
}

impl Aggregator {
    fn get_sql(&self, tuple: &TupleDeclaration) -> String {
        match *self {
            Aggregator::Avg(attribute) => {
                format!("AVG({}.{})", tuple.name, tuple.attributes[attribute].name)
            }
            Aggregator::Sum(attribute) => {
                format!("SUM({}.{})", tuple.name, tuple.attributes[attribute].name)
            }
            Aggregator::Max(attribute) => {
                format!("MAX({}.{})", tuple.name, tuple.attributes[attribute].name)
            }
            Aggregator::Min(attribute) => {
                format!("MIN({}.{})", tuple.name, tuple.attributes[attribute].name)
            }
            Aggregator::Count => "COUNT(*)".to_owned(),
        }
    }
}

impl Predicate {
    fn get_sql(&self, tuple: &TupleDeclaration) -> String {
        let selection;
        let filters = self.tuple
            .constraints
            .iter()
            .map(|expr| expr.get_sql(tuple))
            .collect::<Vec<_>>()
            .join(" AND ");
        let mut rest = String::new();

        match self.ty {
            PredicateType::OrderdStatic { ref parameters, ref ordering } => {
                selection = parameters.iter()
                    .map(|par| par.get_sql(tuple))
                    .collect::<Vec<_>>()
                    .join(", ");
                let order_by = ordering.iter()
                    .map(|ord| ord.get_sql(tuple))
                    .collect::<Vec<_>>()
                    .join(", ");
                rest = format!("ORDER BY {} LIMIT 1", order_by);
            }
            PredicateType::UnorderedStatic { ref parameters } => {
                selection = parameters.iter()
                    .map(|par| par.get_sql(tuple))
                    .collect::<Vec<_>>()
                    .join(", ");
            }
            PredicateType::StaticAggregate { ref aggregator, ref parameter } => {
                selection = format!("{} AS trexaggregate, {}",
                                    aggregator.get_sql(tuple),
                                    parameter.get_sql(tuple));
            }
            PredicateType::StaticNegation => {
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

pub struct SQLiteDriver {
    statement: String,
    pool: Pool<SqliteConnectionManager>,
}

impl SQLiteDriver {
    fn new(predicate: &Predicate,
           tuple: &TupleDeclaration,
           pool: Pool<SqliteConnectionManager>)
           -> Self {
        SQLiteDriver {
            statement: predicate.get_sql(tuple),
            pool: pool,
        }
    }
}

impl Evaluator for SQLiteDriver {
    fn evaluate(&self, result: &PartialResult) -> Vec<PartialResult> {
        // TODO handle errors with Result<_, _>
        let conn = self.pool.get().unwrap();
        let stmt = conn.prepare_cached(&self.statement).unwrap();
        unimplemented!()
    }
}
