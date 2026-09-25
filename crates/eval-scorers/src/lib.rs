//! SDK-owned deterministic scorers. No worker, network, or FFI dependencies.
use serde_json::{json, Value};

#[derive(Clone, Debug)]
enum Expr {
    Literal(Value),
    Path(Vec<String>),
    Call(String, Vec<Expr>),
    Unary(Box<Expr>),
    Binary(String, Box<Expr>, Box<Expr>),
}

struct Parser<'a> {
    text: &'a str,
    pos: usize,
    depth: usize,
    operations: usize,
}
impl<'a> Parser<'a> {
    fn space(&mut self) {
        while self
            .text
            .as_bytes()
            .get(self.pos)
            .is_some_and(u8::is_ascii_whitespace)
        {
            self.pos += 1;
        }
    }
    fn take(&mut self, s: &str) -> bool {
        self.space();
        if self.text[self.pos..].starts_with(s) {
            self.pos += s.len();
            true
        } else {
            false
        }
    }
    fn ident(&mut self) -> Result<String, &'static str> {
        self.space();
        let start = self.pos;
        while self
            .text
            .as_bytes()
            .get(self.pos)
            .is_some_and(|c| c.is_ascii_alphanumeric() || *c == b'_')
        {
            self.pos += 1;
        }
        if start == self.pos {
            return Err("expected identifier");
        }
        Ok(self.text[start..self.pos].into())
    }
    fn expr(&mut self, level: usize) -> Result<Expr, &'static str> {
        if level == 3 {
            return self.atom();
        }
        let ops: &[&str] = match level {
            0 => &["||"],
            1 => &["&&"],
            _ => &["==", "!=", "<=", ">=", "<", ">"],
        };
        let mut left = self.expr(level + 1)?;
        while let Some(op) = ops.iter().find(|op| self.take(op)) {
            self.operations += 1;
            if self.operations > 64 {
                return Err("expression exceeds 64 operators");
            }
            left = Expr::Binary(
                (*op).into(),
                Box::new(left),
                Box::new(self.expr(level + 1)?),
            );
        }
        Ok(left)
    }
    fn atom(&mut self) -> Result<Expr, &'static str> {
        self.depth += 1;
        if self.depth > 32 {
            return Err("expression nesting exceeds 32");
        }
        let value = self.atom_inner();
        self.depth -= 1;
        value
    }
    fn atom_inner(&mut self) -> Result<Expr, &'static str> {
        if self.take("!") {
            return Ok(Expr::Unary(Box::new(self.atom()?)));
        }
        if self.take("(") {
            let x = self.expr(0)?;
            if !self.take(")") {
                return Err("expected closing parenthesis");
            }
            return Ok(x);
        }
        self.space();
        let rest = &self.text[self.pos..];
        if rest.starts_with('"') {
            let mut stream = serde_json::Deserializer::from_str(rest).into_iter::<Value>();
            let value = stream
                .next()
                .ok_or("expected literal")?
                .map_err(|_| "invalid JSON literal")?;
            self.pos += stream.byte_offset();
            return Ok(Expr::Literal(value));
        }
        if rest.starts_with('-') || rest.as_bytes().first().is_some_and(u8::is_ascii_digit) {
            let length = rest
                .bytes()
                .take_while(|c| c.is_ascii_digit() || matches!(c, b'-' | b'+' | b'.' | b'e' | b'E'))
                .count();
            let value =
                serde_json::from_str(&rest[..length]).map_err(|_| "invalid JSON literal")?;
            self.pos += length;
            return Ok(Expr::Literal(value));
        }
        let name = self.ident()?;
        match name.as_str() {
            "true" => return Ok(Expr::Literal(json!(true))),
            "false" => return Ok(Expr::Literal(json!(false))),
            "null" => return Ok(Expr::Literal(Value::Null)),
            _ => {}
        }
        if self.take("(") {
            let mut args = vec![];
            if !self.take(")") {
                loop {
                    args.push(self.expr(0)?);
                    if self.take(")") {
                        break;
                    }
                    if !self.take(",") || args.len() > 2 {
                        return Err("invalid arguments");
                    }
                }
            }
            let arity = if matches!(name.as_str(), "all" | "any") {
                2
            } else if predicate(&name) || matches!(name.as_str(), "size" | "unique") {
                1
            } else {
                return Err("unknown function");
            };
            if args.len() != arity {
                return Err("wrong argument count");
            }
            if arity == 2 && !matches!(&args[1], Expr::Path(p) if p.len()==1 && predicate(&p[0])) {
                return Err("all/any require a type predicate");
            }
            return Ok(Expr::Call(name, args));
        }
        let mut path = vec![name];
        while self.take(".") {
            path.push(self.ident()?);
        }
        if !matches!(
            path[0].as_str(),
            "input" | "output" | "expected" | "input_json" | "output_json" | "expected_json"
        ) && !(path.len() == 1 && predicate(&path[0]))
        {
            return Err("unknown root");
        }
        Ok(Expr::Path(path))
    }
}
fn predicate(name: &str) -> bool {
    matches!(
        name,
        "is_array" | "is_object" | "is_string" | "is_number" | "is_boolean" | "is_null"
    )
}
fn check(name: &str, v: &Value) -> bool {
    match name {
        "is_array" => v.is_array(),
        "is_object" => v.is_object(),
        "is_string" => v.is_string(),
        "is_number" => v.is_number(),
        "is_boolean" => v.is_boolean(),
        "is_null" => v.is_null(),
        _ => false,
    }
}
fn boolean(v: Value) -> Result<bool, &'static str> {
    v.as_bool().ok_or("expected boolean")
}
fn number(v: &Value) -> Result<f64, &'static str> {
    v.as_f64()
        .filter(|v| v.is_finite() && v.abs() <= 9_007_199_254_740_991.0)
        .ok_or("expected a safe finite number")
}
fn equal(a: &Value, b: &Value, budget: &mut usize) -> Result<bool, &'static str> {
    *budget = budget.checked_sub(1).ok_or("evaluation budget exceeded")?;
    if a.is_number() && b.is_number() {
        return Ok(number(a)? == number(b)?);
    }
    match (a, b) {
        (Value::Array(a), Value::Array(b)) => {
            if a.len() != b.len() {
                return Ok(false);
            }
            for (a, b) in a.iter().zip(b) {
                if !equal(a, b, budget)? {
                    return Ok(false);
                }
            }
            Ok(true)
        }
        (Value::Object(a), Value::Object(b)) => {
            if a.len() != b.len() {
                return Ok(false);
            }
            for (k, a) in a {
                match b.get(k) {
                    Some(b) if equal(a, b, budget)? => {}
                    _ => return Ok(false),
                }
            }
            Ok(true)
        }
        _ => Ok(a == b),
    }
}
fn eval(expr: &Expr, input: &Value, budget: &mut usize) -> Result<Value, &'static str> {
    *budget = budget.checked_sub(1).ok_or("evaluation budget exceeded")?;
    match expr {
        Expr::Literal(v) => Ok(v.clone()),
        Expr::Path(path) => {
            let root = &path[0];
            let key = root.strip_suffix("_json").unwrap_or(root);
            let mut value = input.get(key).cloned().unwrap_or(Value::Null);
            if root.ends_with("_json") {
                if let Value::String(s) = value {
                    value = serde_json::from_str(&s).map_err(|_| "invalid encoded JSON")?;
                    if !valid_structure(&value) {
                        return Err("JSON structure exceeds limits");
                    }
                }
            }
            for key in &path[1..] {
                value = value
                    .as_object()
                    .and_then(|v| v.get(key))
                    .cloned()
                    .ok_or("missing input field")?;
            }
            Ok(value)
        }
        Expr::Unary(x) => Ok(json!(!boolean(eval(x, input, budget)?)?)),
        Expr::Binary(op, a, b) => {
            let a = eval(a, input, budget)?;
            if op == "&&" {
                return Ok(json!(boolean(a)? && boolean(eval(b, input, budget)?)?));
            }
            if op == "||" {
                return Ok(json!(boolean(a)? || boolean(eval(b, input, budget)?)?));
            }
            let b = eval(b, input, budget)?;
            Ok(json!(match op.as_str() {
                "==" => equal(&a, &b, budget)?,
                "!=" => !equal(&a, &b, budget)?,
                "<" => number(&a)? < number(&b)?,
                ">" => number(&a)? > number(&b)?,
                "<=" => number(&a)? <= number(&b)?,
                ">=" => number(&a)? >= number(&b)?,
                _ => unreachable!(),
            }))
        }
        Expr::Call(name, args) => {
            let v = eval(&args[0], input, budget)?;
            if predicate(name) {
                return Ok(json!(check(name, &v)));
            }
            if name == "size" {
                return Ok(json!(match &v {
                    Value::Array(v) => v.len(),
                    Value::Object(v) => v.len(),
                    Value::String(v) => v.chars().count(),
                    _ => return Err("size requires array, object, or string"),
                }));
            }
            let items = v.as_array().ok_or("function requires array")?;
            if items.len() > 4096 {
                return Err("array exceeds 4096 elements");
            }
            if name == "unique" {
                for (i, a) in items.iter().enumerate() {
                    for b in &items[..i] {
                        *budget = budget.checked_sub(1).ok_or("evaluation budget exceeded")?;
                        if equal(a, b, budget)? {
                            return Ok(json!(false));
                        }
                    }
                }
                return Ok(json!(true));
            }
            let Expr::Path(p) = &args[1] else {
                unreachable!()
            };
            Ok(json!(if name == "all" {
                items.iter().all(|v| check(&p[0], v))
            } else {
                items.iter().any(|v| check(&p[0], v))
            }))
        }
    }
}
fn error(label: &str, message: &str, assertions: Vec<Value>) -> Value {
    json!({"score":0.0,"passed":false,"label":label,"explanation":message,"metadata":{"assertions":assertions}})
}

fn valid_structure(input: &Value) -> bool {
    let mut pending = vec![(input, 0)];
    let mut nodes = 0;
    while let Some((value, depth)) = pending.pop() {
        nodes += 1;
        if depth > 64 || nodes > 100_000 {
            return false;
        }
        match value {
            Value::Array(v) => pending.extend(v.iter().map(|v| (v, depth + 1))),
            Value::Object(v) => pending.extend(v.values().map(|v| (v, depth + 1))),
            _ => {}
        }
    }
    true
}

/// Execute the version-one bounded JSON assertion language. Never evaluates host code.
pub fn structured_assertions(input: &Value) -> Value {
    if !valid_structure(input) {
        return error("input_error", "JSON structure exceeds limits", vec![]);
    }
    let Some(config) = input.get("config").and_then(Value::as_object) else {
        return error("config_error", "config must be an object", vec![]);
    };
    let Some(assertions) = config
        .get("assertions")
        .and_then(Value::as_array)
        .filter(|v| !v.is_empty() && v.len() <= 64)
    else {
        return error(
            "config_error",
            "assertions must contain 1..64 entries",
            vec![],
        );
    };
    let threshold = match config.get("score_threshold") {
        None => 1.0,
        Some(v) => match v.as_f64().filter(|v| (0.0..=1.0).contains(v)) {
            Some(v) => v,
            None => {
                return error(
                    "config_error",
                    "score_threshold must be between 0 and 1",
                    vec![],
                )
            }
        },
    };
    let mut compiled = vec![];
    let mut names = std::collections::HashSet::new();
    for (i, a) in assertions.iter().enumerate() {
        let name = match a.get("name") {
            None => format!("assertion_{}", i + 1),
            Some(Value::String(s)) if !s.is_empty() && s.len() <= 256 => s.clone(),
            _ => return error("config_error", "invalid assertion name", vec![]),
        };
        if !names.insert(name.clone()) {
            return error("config_error", "duplicate assertion name", vec![]);
        }
        let Some(expr) = a
            .get("expr")
            .and_then(Value::as_str)
            .filter(|s| !s.is_empty() && s.len() <= 4096)
        else {
            return error(
                "config_error",
                "expression must contain 1..4096 bytes",
                vec![],
            );
        };
        let mut parser = Parser {
            text: expr,
            pos: 0,
            depth: 0,
            operations: 0,
        };
        let tree = match parser.expr(0) {
            Ok(tree) => tree,
            Err(e) => return error("config_error", e, vec![]),
        };
        parser.space();
        if parser.pos != expr.len() {
            return error("config_error", "unexpected expression suffix", vec![]);
        }
        compiled.push((name, expr, tree));
    }
    if serde_json::to_vec(input).map_or(true, |v| v.len() > 1_048_576) {
        return error("input_error", "input exceeds 1 MiB", vec![]);
    }
    let mut results = vec![];
    let mut passed = 0;
    let mut budget = 100_000;
    for (name, expr, tree) in compiled {
        match eval(&tree, input, &mut budget).and_then(boolean) {
            Ok(ok) => {
                passed += usize::from(ok);
                results.push(json!({"name":name,"expr":expr,"passed":ok}));
            }
            Err(e) => {
                results.push(json!({"name":name,"expr":expr,"passed":false,"error":e}));
                return error("input_error", e, results);
            }
        }
    }
    let score = passed as f64 / assertions.len() as f64;
    json!({"score":score,"passed":score>=threshold,"label":if score>=threshold {"pass"} else {"fail"},"explanation":format!("{passed}/{} assertions passed",assertions.len()),"metadata":{"assertions":results}})
}
