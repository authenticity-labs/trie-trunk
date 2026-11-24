//! Query syntax is lowered to one application plan. No codec or storage logic.
pub mod completion;
pub mod taxonomy;
mod join;
use serde_json::{Value as Json, json};
use sqlparser::{ast::*, dialect::AnsiDialect, parser::Parser};
use std::collections::HashSet;

pub use crate::relational::{Selection, Relational, reduce};
/// The same language operations serve native sockets and the WASM adapter.
/// Parsing and completion never depend on a browser or a storage transport.
pub fn request(v: &Json) -> Result<Json, String> {
    match v["op"].as_str() {
        Some("prepare") => {
            let fields: Vec<String> = serde_json::from_value(v["fields"].clone()).map_err(|e|e.to_string())?;
            parse_execution(v["text"].as_str().ok_or("Missing query")?,&fields,None,true)
        },
        Some("parse") => {
            let fields: Vec<String> = serde_json::from_value(v["fields"].clone())
                .map_err(|e| e.to_string())?;
            parse(v["text"].as_str().ok_or("Missing query")?, &fields)
        }
        Some("complete") => {
            let fields: Vec<completion::Field> = serde_json::from_value(v["fields"].clone())
                .map_err(|e| e.to_string())?;
            let caret = usize::try_from(v["caret"].as_u64().ok_or("Missing caret")?)
                .map_err(|_| "Caret exceeds platform size")?;
            completion::complete(v["text"].as_str().ok_or("Missing query")?, caret, &fields)
        }
        Some("reduce") => reduce(&v["plan"], v["rows"].as_array().ok_or("Missing rows")?),
        _ => Err("Unknown query operation".into()),
    }
}
fn name(e: &Expr) -> Result<String, String> {
    match e {
        Expr::Identifier(i) => Ok(if i.quote_style.is_some() {
            i.value.clone()
        } else {
            i.value.to_lowercase()
        }),
        _ => Err("Use an unqualified field name, or quote a qualified metadata name".into()),
    }
}
fn operand(e:&Expr)->Result<(String,Option<crate::cast::Spec>),String> {
    if let Expr::Cast{kind,expr,data_type,format}=e {
        if format.is_some() || !matches!(kind,CastKind::Cast|CastKind::TryCast){return Err("Unsupported CAST syntax".into());}
        use crate::cast::Target::*;
        let (target,bytes)=match data_type.to_string().as_str(){
            "TINYINT"=>(Int,1),"SMALLINT"=>(Int,2),"INT"|"INTEGER"=>(Int,4),"BIGINT"=>(Int,8),
            "REAL"=>(Float,4),"DOUBLE"|"DOUBLE PRECISION"|"FLOAT"=>(Float,8),"BOOL"|"BOOLEAN"=>(Bool,1),
            _=>return Err(format!("Unsupported CAST target {data_type}")),
        };
        Ok((name(expr)?,Some(crate::cast::Spec{target,bytes,tolerant:matches!(kind,CastKind::TryCast)})))
    }else{Ok((name(e)?,None))}
}
fn cast_filter(field:&str,cast:Option<crate::cast::Spec>,op:&str,value:Json)->Json {
    let mut p=filter(field,op,value);if let Some(cast)=cast {p["filter"]["cast"]=json!(cast);}p
}
fn filter(field: &str, op: &str, value: Json) -> Json {
    json!({"kind":"filter","filter":{"field":field,"op":op,"value":value}})
}
fn both(kind: &str, left: Json, right: Json) -> Json {
    if left == right {
        return left;
    }
    if left["kind"] == kind && (left["left"] == right || left["right"] == right) {
        return left;
    }
    if right["kind"] == kind && (right["left"] == left || right["right"] == left) {
        return right;
    }
    json!({"kind":kind,"left":left,"right":right})
}
fn null() -> Json {
    json!({"kind":"null","value":""})
}
fn literal(e: &Expr) -> Result<Json, String> {
    match e {
        Expr::Value(v) => match &v.value {
            Value::Number(n, _) => Ok(json!({"kind":"number","value":n})),
            Value::SingleQuotedString(s) => Ok(json!({"kind":"string","value":s})),
            Value::Boolean(b) => Ok(json!({"kind":"boolean","value":b.to_string()})),
            Value::Null => Ok(null()),
            _ => Err("Unsupported SQL literal".into()),
        },
        Expr::UnaryOp {
            op: UnaryOperator::Minus,
            expr,
        } => {
            let mut v = literal(expr)?;
            if v["kind"] != "number" {
                return Err("Minus requires a number".into());
            }
            v["value"] = json!(format!("-{}", v["value"].as_str().unwrap()));
            Ok(v)
        }
        _ => Err("Comparisons require a field and a literal".into()),
    }
}
// Carry both true and false conditions. NOT swaps them, so NULL/absent never
// becomes true merely because a Boolean result was inverted in the older host.
fn truth(e: &Expr, fields: &HashSet<String>) -> Result<(Json, Json), String> {
    let checked = |e: &Expr| -> Result<(String,Option<crate::cast::Spec>), String> {
        let (n,cast) = operand(e)?;
        if !fields.contains(&n) {
            return Err(format!("Unknown field {n}"));
        }
        Ok((n,cast))
    };
    match e {
        Expr::Nested(e) => truth(e, fields),
        Expr::UnaryOp {
            op: UnaryOperator::Not,
            expr,
        } => {
            let (t, f) = truth(expr, fields)?;
            Ok((f, t))
        }
        Expr::BinaryOp {
            left,
            op: BinaryOperator::And,
            right,
        } => {
            let (a, b) = truth(left, fields)?;
            let (c, d) = truth(right, fields)?;
            Ok((both("and", a, c), both("or", b, d)))
        }
        Expr::BinaryOp {
            left,
            op: BinaryOperator::Or,
            right,
        } => {
            let (a, b) = truth(left, fields)?;
            let (c, d) = truth(right, fields)?;
            Ok((both("or", a, c), both("and", b, d)))
        }
        Expr::IsNull(e) => {
            let (n,cast) = checked(e)?;
            Ok((cast_filter(&n,cast, "==", null()), cast_filter(&n,cast, "!=", null())))
        }
        Expr::IsNotNull(e) => {
            let (n,cast) = checked(e)?;
            Ok((cast_filter(&n,cast, "!=", null()), cast_filter(&n,cast, "==", null())))
        }
        Expr::BinaryOp { left, op, right } => {
            let (n,cast) = checked(left)?;
            let v = literal(right)?;
            let (yes, no) = match op {
                BinaryOperator::Eq => ("==", "!="),
                BinaryOperator::NotEq => ("!=", "=="),
                BinaryOperator::Gt => (">", "<="),
                BinaryOperator::GtEq => (">=", "<"),
                BinaryOperator::Lt => ("<", ">="),
                BinaryOperator::LtEq => ("<=", ">"),
                _ => return Err("Unsupported SQL comparison".into()),
            };
            let present = cast_filter(&n,cast, "!=", null());
            if v["kind"] == "null" {
                let never = both("and", present, cast_filter(&n,cast, "==", null()));
                return Ok((never.clone(), never));
            }
            if cast.is_some(){return Ok((cast_filter(&n,cast,yes,v.clone()),cast_filter(&n,cast,no,v)));}
            Ok((
                both("and", present.clone(), cast_filter(&n,cast, yes, v.clone())),
                both("and", present, cast_filter(&n,cast, no, v)),
            ))
        }
        _ => Err("Unsupported WHERE expression".into()),
    }
}
pub fn parse(text: &str, fields: &[String]) -> Result<Json, String> {
    parse_scoped(text, fields, None)
}

pub fn parse_scoped(
    text: &str,
    fields: &[String],
    catalog: Option<&taxonomy::Scope<'_>>,
) -> Result<Json, String> {
    parse_execution(text,fields,catalog,false)
}
fn parse_execution(text:&str,fields:&[String],catalog:Option<&taxonomy::Scope<'_>>,execution:bool)->Result<Json,String> {
    if text.len() > 8192 {
        return Err("Query too long".into());
    }
    let statements = Parser::new(&AnsiDialect {})
        .with_recursion_limit(64)
        .try_with_sql(text)
        .map_err(|e| e.to_string())?
        .parse_statements()
        .map_err(|e| e.to_string())?;
    if statements.len() != 1 {
        return Err("Provide one SELECT statement".into());
    }
    let Statement::Query(q) = &statements[0] else {
        return Err("Only SELECT is supported".into());
    };
    if q.with.is_some()
        || q.limit_clause.is_some()
        || q.fetch.is_some()
        || !q.locks.is_empty()
        || q.for_clause.is_some()
        || q.settings.is_some()
        || q.format_clause.is_some()
        || !q.pipe_operators.is_empty()
    {
        return Err("Unsupported SELECT clause".into());
    }
    let SetExpr::Select(s) = q.body.as_ref() else {
        return Err("Set operations and subqueries are not supported".into());
    };
    if s.distinct.is_some()
        || s.top.is_some()
        || s.into.is_some()
        || s.exclude.is_some()
        || s.having.is_some()
        || s.qualify.is_some()
        || s.prewhere.is_some()
        || s.select_modifiers.is_some()
        || !s.optimizer_hints.is_empty()
        || !s.lateral_views.is_empty()
        || !s.connect_by.is_empty()
        || !s.cluster_by.is_empty()
        || !s.distribute_by.is_empty()
        || !s.sort_by.is_empty()
        || !s.named_window.is_empty()
        || s.value_table_mode.is_some()
    {
        return Err("Unsupported SELECT clause".into());
    }
    if execution && s.from.len() == 1 && !s.from[0].joins.is_empty() {
        if q.order_by.is_some() { return Err("Ordering joined rows is not supported yet".into()); }
        return Ok(json!({"collection":"events","plan":{"join":join::parse(s)?}}));
    }
    if s.from.len() != 1 || !s.from[0].joins.is_empty() {
        return Err("Joins are not supported".into());
    }
    let TableFactor::Table {
        name: table,
        alias: None,
        args: None,
        with_hints,
        version: None,
        partitions,
        ..
    } = &s.from[0].relation
    else {
        return Err("Use a registered event source without table aliases".into());
    };
    if s.from[0].relation.to_string() != table.to_string()
        || !with_hints.is_empty()
        || !partitions.is_empty()
    {
        return Err("Unsupported table clause".into());
    }
    let parts = table
        .0
        .iter()
        .map(|p| {
            let i = p.as_ident().ok_or("Table functions are not supported")?;
            Ok(if i.quote_style.is_some() {
                i.value.clone()
            } else {
                i.value.to_lowercase()
            })
        })
        .collect::<Result<Vec<_>, String>>()?;
    if parts.len() == 1 && parts[0] == "summaries" { return Err("Unknown query source".into()); }
    let generic = parts.len() == 1 && parts[0] == "events";
    let target = if generic {
        None
    } else {
        Some(
            catalog
                .ok_or("Qualified tables require a registered taxonomy")?
                .resolve(&parts)?,
        )
    };
    let source = if generic { parts[0].as_str() } else { "events" };
    let names: HashSet<String> = fields.iter().cloned().collect();
    let mut selection = vec![];
    let mut aliases = HashSet::new();
    for item in &s.projection {
        if let SelectItem::Wildcard(options) = item {
            if s.projection.len() != 1 || options.to_string() != "" {
                return Err("Use SELECT * without wildcard modifiers".into());
            }
            continue;
        }
        let (e, alias) = match item {
            SelectItem::UnnamedExpr(e) => (e, None),
            SelectItem::ExprWithAlias { expr, alias } => (expr, Some(alias.value.clone())),
            _ => return Err("Unsupported projection".into()),
        };
        let (field, function, cast) = if let Expr::Function(f) = e {
            let op = f.name.to_string().to_lowercase();
            if !["count", "sum", "min", "max", "avg"].contains(&op.as_str()) {
                return Err(format!(
                    "Aggregate {op} is not supported by the summary reader"
                ));
            }
            if f.filter.is_some()
                || f.over.is_some()
                || !f.within_group.is_empty()
                || f.null_treatment.is_some()
                || f.uses_odbc_syntax
                || !matches!(f.parameters, FunctionArguments::None)
            {
                return Err("Unsupported aggregate modifier".into());
            }
            let FunctionArguments::List(args) = &f.args else {
                return Err("Aggregate requires one argument".into());
            };
            if args.args.len() != 1
                || args.duplicate_treatment.is_some()
                || !args.clauses.is_empty()
            {
                return Err("Aggregate requires one argument without DISTINCT".into());
            }
            let field = match &args.args[0] {
                FunctionArg::Unnamed(FunctionArgExpr::Wildcard) if op == "count" => None,
                FunctionArg::Unnamed(FunctionArgExpr::Expr(e)) => Some(name(e)?),
                _ => return Err("Unsupported aggregate argument".into()),
            };
            (field, Some(op), None)
        } else {
            let (field,cast)=operand(e)?;
            (Some(field), None, cast)
        };
        if let Some(n) = &field {
            if !names.contains(n) {
                return Err(format!("Unknown field {n}"));
            }
        }
        let alias = alias.unwrap_or_else(|| {
            function
                .as_ref()
                .map(|f| format!("{f}({})", field.as_deref().unwrap_or("*")))
                .unwrap_or_else(|| field.clone().unwrap())
        });
        if !aliases.insert(alias.clone()) {
            return Err(format!("Duplicate result column {alias}"));
        }
        selection.push(Selection {
            field,
            function,
            alias,
            cast,
        });
    }
    let groups = match &s.group_by {
        GroupByExpr::Expressions(exprs, mods) if mods.is_empty() => exprs,
        _ => return Err("Unsupported grouping clause".into()),
    };
    let aggregate = selection.iter().any(|s| s.function.is_some());
    let group_by = if groups.is_empty() {
        aggregate.then(Vec::new)
    } else {
        Some(groups.iter().map(name).collect::<Result<Vec<_>, _>>()?)
    };
    if group_by.is_some() && selection.iter().any(|s|s.cast.is_some()){return Err("CAST projections currently require event rows without grouping".into());}

    if group_by.is_some() && !aggregate {
        return Err("GROUP BY requires an aggregate selection".into());
    }
    if let Some(groups) = &group_by {
        for g in groups {
            if !names.contains(g) {
                return Err(format!("Unknown group field {g}"));
            }
        }
        for s in &selection {
            if s.function.is_none() && !groups.contains(s.field.as_ref().unwrap()) {
                return Err(format!(
                    "Selected field {} must appear in GROUP BY",
                    s.alias
                ));
            }
        }
    }
    let mut plan = json!({"filters":[],"order":"asc","relational":Relational{selection,group_by}});
    if let Some(target) = target {
        plan["table"] = json!(target);
    }
    if let Some(e) = &s.selection {
        plan["predicate"] = truth(e, &names)?.0
    }
    if let Some(order) = &q.order_by {
        let OrderByKind::Expressions(v) = &order.kind else {
            return Err("Unsupported ordering".into());
        };
        if v.len() != 1
            || order.interpolate.is_some()
            || v[0].options.nulls_first.is_some()
            || v[0].with_fill.is_some()
            || matches!(v[0].options.sort, Some(OrderBySort::Using(_)))
            || source != "events"
            || name(&v[0].expr)? != "timestamp"
            || aggregate
        {
            return Err("Only event timestamp ordering is currently supported".into());
        }
        plan["order"] = json!(if matches!(v[0].options.sort, Some(OrderBySort::Desc)) {
            "desc"
        } else {
            "asc"
        });
        if execution { plan["orderBy"] = json!({"field":"timestamp","direction":plan["order"]}); }
    }
    Ok(json!({"collection":source,"plan":plan}))
}


#[cfg(test)]
mod tests {
    use super::*;
    use crate::relational::{Num, matches};
    fn fields() -> Vec<String> {
        ["time", "timestamp", "value", "region", "bucket", "count"]
            .map(str::to_owned)
            .to_vec()
    }
    #[test]
    fn aggregate_groups_and_projections() {
        assert_eq!(parse("SELECT COUNT(*) FROM events", &fields()).unwrap()["plan"]["relational"]["groupBy"], json!([]));
        assert!(parse("SELECT region, COUNT(*) FROM events", &fields()).unwrap_err().contains("must appear in GROUP BY"));
        let p=parse("SELECT region, COUNT(*), SUM(value), MIN(value), MAX(value), AVG(value) FROM events GROUP BY region",&fields()).unwrap();
        assert_eq!(p["plan"]["relational"]["groupBy"], json!(["region"]));
        assert_eq!(
            p["plan"]["relational"]["selection"]
                .as_array()
                .unwrap()
                .len(),
            6
        );
        assert!(parse("SELECT value FROM events", &fields()).is_ok());
        assert!(parse("SELECT * FROM events", &fields()).is_ok());
    }
    #[test]
    fn unsupported_clauses_never_disappear() {
        for q in [
            "SELECT * FROM summaries",
            "SELECT bucket, SUM(count) FROM summaries GROUP BY bucket",
            "SELECT * FROM events JOIN summaries ON events.time=summaries.bucket",
            "SELECT value, COUNT(*) FROM events GROUP BY region",
            "SELECT SUM(value) FROM events GROUP BY ()",
            "SELECT DISTINCT region FROM events",
            "SELECT * FROM events LIMIT 10",
            "SELECT * FROM events ORDER BY timestamp USING >",
            "SELECT * FROM events; SELECT * FROM summaries",
            "SELECT value FROM events WHERE value IN (1,2)",
            "SELECT * FROM events FOR UPDATE",
            "SELECT COUNT(DISTINCT region) FROM events GROUP BY value",
        ] {
            assert!(parse(q, &fields()).is_err(), "{q}");
        }
    }
    #[test]
    fn null_unknown_is_not_inverted() {
        let p = parse("SELECT * FROM events WHERE NOT(value = NULL)", &fields()).unwrap();
        for row in [
            json!({"values":{"value":1}}),
            json!({"values":{"value":null}}),
            json!({"count":1,"measures":{"value":{"count":0,"nulls":0}}}),
        ] {
            assert!(!matches(&row, &p["plan"]["predicate"]).unwrap());
        }
    }
    #[test]
    fn exact_numeric_comparison() {
        assert!(
            Num::Int(9_007_199_254_740_993)
                .cmp(Num::Float(9_007_199_254_740_992.0))
                .is_gt()
        );
        assert!(Num::Int(-1).cmp(Num::Float(-1.5)).is_gt());
        assert!(
            Num::Int(i128::MAX)
                .cmp(Num::Float(i128::MAX as f64))
                .is_lt()
        );
    }
}
