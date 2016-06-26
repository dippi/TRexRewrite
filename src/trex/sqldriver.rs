use tesla::*;
use tesla::expressions::*;
use tesla::predicates::*;
use trex::stacks::*;
use trex::expressions::*;
use linear_map::LinearMap;
use rusqlite::Row;
use rusqlite::types::{ToSql, Value as SqlValue};
use r2d2::Pool;
use r2d2_sqlite::SqliteConnectionManager;

struct SqlContext<'a> {
    idx: usize,
    tuple: &'a TupleDeclaration,
    parameters: Vec<String>,
    aggregate: Option<String>,
}

impl<'a> SqlContext<'a> {
    fn new(idx: usize, tuple: &'a TupleDeclaration) -> Self {
        SqlContext {
            idx: idx,
            tuple: tuple,
            parameters: Vec::new(),
            aggregate: None,
        }
    }

    fn set_aggregate(&mut self, aggr: &Aggregator) {
        let sql = match *aggr {
            Aggregator::Avg(attribute) => {
                format!("AVG({}.{})",
                        self.tuple.name,
                        self.tuple.attributes[attribute].name)
            }
            Aggregator::Sum(attribute) => {
                format!("SUM({}.{})",
                        self.tuple.name,
                        self.tuple.attributes[attribute].name)
            }
            Aggregator::Max(attribute) => {
                format!("MAX({}.{})",
                        self.tuple.name,
                        self.tuple.attributes[attribute].name)
            }
            Aggregator::Min(attribute) => {
                format!("MIN({}.{})",
                        self.tuple.name,
                        self.tuple.attributes[attribute].name)
            }
            Aggregator::Count => "COUNT(*)".to_owned(),
        };
        self.aggregate = Some(sql);
    }

    fn insert_parameter(&mut self, param: &ParameterDeclaration) -> String {
        let sql = self.encode_expression(&param.expression);
        self.parameters.push(sql.clone());
        sql
    }

    fn encode_value(&self, value: &Value) -> String {
        match *value {
            Value::Int(value) => format!("{}", value),
            Value::Float(value) => format!("{}", value),
            Value::Bool(value) => format!("{}", value),
            // TODO check excaping for SQL injection
            Value::Str(ref value) => format!("{:?}", value),
        }
    }

    fn encode_unary(&self, op: &UnaryOperator) -> String {
        match *op {
                UnaryOperator::Minus => "-",
                UnaryOperator::Not => "!",
            }
            .to_owned()
    }

    fn encode_binary(&self, op: &BinaryOperator) -> String {
        match *op {
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

    fn encode_attribute(&self, attribute: usize) -> String {
        format!("{}.{}",
                self.tuple.name,
                self.tuple.attributes[attribute].name)
    }

    fn get_parameter(&self, predicate: usize, parameter: usize) -> String {
        if predicate == self.idx {
            self.parameters[parameter].clone()
        } else {
            format!(":param{}x{}", predicate, parameter)
        }
    }

    fn encode_expression(&self, expr: &Expression) -> String {
        match *expr {
            Expression::Immediate { ref value } => self.encode_value(value),
            Expression::Reference { attribute } => self.encode_attribute(attribute),
            Expression::Parameter { predicate, parameter } => {
                self.get_parameter(predicate, parameter)
            }
            Expression::Aggregate => self.aggregate.clone().unwrap(),
            Expression::Cast { ref expression, .. } => self.encode_expression(expression),
            Expression::UnaryOperation { ref operator, ref expression } => {
                format!("({}{})",
                        self.encode_unary(operator),
                        self.encode_expression(expression))
            }
            Expression::BinaryOperation { ref operator, ref left, ref right } => {
                format!("({} {} {})",
                        self.encode_expression(left),
                        self.encode_binary(operator),
                        self.encode_expression(right))
            }
        }
    }

    fn encode_order(&self, ord: &Order) -> String {
        match *ord {
                Order::Asc => "ASC",
                Order::Desc => "DESC",
            }
            .to_owned()
    }

    fn encode_ordering(&self, ord: &Ordering) -> String {
        format!("{}.{} {}",
                self.tuple.name,
                self.tuple.attributes[ord.attribute].name,
                self.encode_order(&ord.direction))
    }

    fn encode_predicate(&mut self, pred: &Predicate) -> String {
        let selection;
        let filters = pred.tuple
            .constraints
            .iter()
            .map(|expr| self.encode_expression(expr))
            .collect::<Vec<_>>()
            .join(" AND ");
        let mut rest = String::new();

        match pred.ty {
            PredicateType::OrderdStatic { ref parameters, ref ordering } => {
                selection = parameters.iter()
                    .map(|par| {
                        let sql = self.insert_parameter(par);
                        format!("{} AS {}", sql, par.name)
                    })
                    .collect::<Vec<_>>()
                    .join(", ");
                let order_by = ordering.iter()
                    .map(|ord| self.encode_ordering(ord))
                    .collect::<Vec<_>>()
                    .join(", ");
                rest = format!("ORDER BY {} LIMIT 1", order_by);
            }
            PredicateType::UnorderedStatic { ref parameters } => {
                selection = parameters.iter()
                    .map(|par| {
                        let sql = self.insert_parameter(par);
                        format!("{} AS {}", sql, par.name)
                    })
                    .collect::<Vec<_>>()
                    .join(", ");
            }
            PredicateType::StaticAggregate { ref aggregator, ref parameter } => {
                self.set_aggregate(aggregator);
                let sql = self.insert_parameter(parameter);
                selection = format!("{} AS {}", sql, parameter.name);
            }
            PredicateType::StaticNegation => {
                selection = "1".to_owned();
                rest = "LIMIT 1".to_owned();
            }
            _ => panic!("Error composing the SQL statement"),
        }

        format!("SELECT {} FROM {} WHERE {} {}",
                if !selection.is_empty() { selection } else { "1".to_owned() },
                self.tuple.name,
                filters,
                rest)
    }
}

pub struct SQLiteDriver {
    idx: usize,
    predicate: Predicate,
    input_params: Vec<(usize, usize)>,
    output_params: Vec<BasicType>,
    statement: String,
    pool: Pool<SqliteConnectionManager>,
}

impl SQLiteDriver {
    pub fn new(idx: usize,
               tuple: &TupleDeclaration,
               predicate: &Predicate,
               parameters_ty: &LinearMap<(usize, usize), BasicType>,
               pool: Pool<SqliteConnectionManager>)
               -> Option<Self> {
        if let TupleType::Static = tuple.ty {
            let mut input_params = predicate.tuple
                .constraints
                .iter()
                .flat_map(|expr| expr.get_parameters())
                .filter(|&(param, _)| param != idx)
                .collect::<Vec<_>>();
            input_params.sort();
            input_params.dedup();
            let output_params = match predicate.ty {
                PredicateType::OrderdStatic { ref parameters, .. } |
                PredicateType::UnorderedStatic { ref parameters } => {
                    (0..parameters.len()).map(|n| parameters_ty[&(idx, n)].clone()).collect()
                }
                PredicateType::StaticAggregate { .. } => vec![parameters_ty[&(idx, 0)].clone()],
                _ => Vec::new(),
            };
            let statement = SqlContext::new(idx, tuple).encode_predicate(predicate);
            Some(SQLiteDriver {
                idx: idx,
                predicate: predicate.clone(),
                input_params: input_params,
                output_params: output_params,
                statement: statement,
                pool: pool,
            })
        } else {
            None
        }
    }
}

// FIXME shouldn't be needed as soon as rusqlite is updated with the new ToSql trait
fn to_sql_value(value: &Value) -> SqlValue {
    match *value {
        Value::Int(x) => SqlValue::Integer(x.into()),
        Value::Float(x) => SqlValue::Real(x.into()),
        Value::Bool(x) => SqlValue::Integer(if x { 1 } else { 0 }),
        Value::Str(ref x) => SqlValue::Text(x.clone()),
    }
}

// FIXME shouldn't be needed as soon as rusqlite is updated with the new ToSql trait
fn to_sql_ref(value: &SqlValue) -> &ToSql {
    match *value {
        SqlValue::Integer(ref x) => x,
        SqlValue::Real(ref x) => x,
        SqlValue::Text(ref x) => x,
        _ => unreachable!(),
    }
}

fn get_res(row: &Row, i: i32, ty: &BasicType) -> Value {
    match *ty {
        BasicType::Int => Value::Int(row.get::<_, i64>(i) as i32),
        BasicType::Float => Value::Float(row.get::<_, f64>(i) as f32),
        BasicType::Bool => Value::Bool(row.get::<_, i64>(i) != 0),
        BasicType::Str => Value::Str(row.get(i)),
    }
}

impl EventProcessor for SQLiteDriver {
    fn evaluate(&self, result: &PartialResult) -> Vec<PartialResult> {
        // TODO handle errors with Result<_, _>
        let context = CompleteContext::new(&result, ());
        let conn = self.pool.get().unwrap();
        let mut stmt = conn.prepare_cached(&self.statement).unwrap();
        let owned_params = self.input_params
            .iter()
            .map(|&(pred, par)| {
                let name = format!(":param{}x{}", pred, par);
                let value = to_sql_value(&context.get_parameter(pred, par));
                (name, value)
            })
            .collect::<Vec<_>>();
        let ref_params = owned_params.iter()
            .map(|&(ref name, ref value)| (name as &str, to_sql_ref(value)))
            .collect::<Vec<_>>();
        match self.predicate.ty {
            PredicateType::OrderdStatic { .. } |
            PredicateType::UnorderedStatic { .. } => {
                stmt.query_map_named(&ref_params, |row| {
                        self.output_params
                            .iter()
                            .enumerate()
                            .fold(result.clone(), |result, (i, ty)| {
                                let value = get_res(row, i as i32, ty);
                                result.insert_parameter((self.idx, i), value)
                            })
                    })
                    .unwrap()
                    .map(Result::unwrap)
                    .collect()
            }
            PredicateType::StaticAggregate { .. } => {
                stmt.query_map_named(&ref_params, |row| {
                        let value = get_res(row, 1, &self.output_params[0]);
                        result.clone().insert_parameter((self.idx, 0), value)
                    })
                    .unwrap()
                    .map(Result::unwrap)
                    .collect()
            }
            PredicateType::StaticNegation { .. } => {
                let exists = stmt.query_named(&ref_params).unwrap().next().is_some();
                if !exists { vec![result.clone()] } else { Vec::new() }
            }
            _ => unreachable!(),
        }
    }
}
