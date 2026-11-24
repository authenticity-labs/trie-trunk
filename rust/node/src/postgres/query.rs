//! Bind SQL relation names to registered event sources before shared SQL planning.
use super::{
    catalog::{Catalog, Table},
    *,
};
use serde_json::json;
use sqlparser::{ast::*, dialect::PostgreSqlDialect, parser::Parser};
use std::ops::ControlFlow;

pub type Columns = Vec<(String, Type)>;
pub enum Rows {
    Catalog(std::vec::IntoIter<Vec<Option<String>>>),
    Aggregate {
        branches: Vec<u16>,
        plan: serde_json::Value,
        offset: usize,
        limit: Option<usize>,
        cancel: Arc<AtomicBool>,
    },
    Scan(data_seeds::Cursor),
    Join(data_seeds::relational::join::Join),
    Describe,
}
pub struct Output {
    pub columns: Columns,
    pub keys: Vec<String>,
    pub rows: Rows,
    pub skip: usize,
}

fn parts(n: &ObjectName) -> Vec<String> {
    n.0.iter()
        .filter_map(|i| i.as_ident())
        .map(|i| {
            if i.quote_style.is_some() {
                i.value.clone()
            } else {
                i.value.to_lowercase()
            }
        })
        .collect()
}
fn relation<'a>(t: &mut TableFactor, cat: &'a Catalog, join: bool) -> Result<&'a Table> {
    let TableFactor::Table { name, .. } = t else {
        return Err("Expected a registered table".into());
    };
    let parts = parts(name);
    let (schema, table) = match parts.as_slice() {
        [t] => (cat.schema(), t),
        [s, t] => (s.as_str(), t),
        _ => return Err("Expected schema.table in the connected cache".into()),
    };
    let t = cat
        .tables
        .get(&(schema.to_owned(), table.clone()))
        .ok_or_else(|| format!("Unknown event table {schema}.{table}"))?;
    *name = if join {
        ObjectName::from(
            t.path
                .iter()
                .map(|s| Ident::with_quote('"', s))
                .collect::<Vec<_>>(),
        )
    } else {
        ObjectName::from(Ident::new("events"))
    };
    Ok(t)
}
fn page(e: &Expr) -> Result<usize> {
    let Expr::Value(v) = e else {
        return Err("Expected a nonnegative LIMIT or OFFSET".into());
    };
    let text = match &v.value {
        Value::Number(s, _) | Value::SingleQuotedString(s) => s,
        _ => return Err("Invalid pagination".into()),
    };
    Ok(text
        .parse::<usize>()
        .map_err(|_| "Expected a nonnegative LIMIT or OFFSET")?)
}

pub fn prepare(
    node: &Node,
    db: &str,
    pid: i32,
    sql: &str,
    params: &[Option<String>],
    describe: bool,
    active: &Arc<Active>,
) -> Result<Output> {
    if sql.len() > 64 * 1024 {
        return Err("SQL exceeds adapter limit".into());
    }
    let mut statements = Parser::new(&PostgreSqlDialect {})
        .with_recursion_limit(64)
        .try_with_sql(sql)?
        .parse_statements()?;
    if statements.len() != 1 {
        return Err("Send one SELECT statement".into());
    }
    let Statement::Query(mut query) = statements.pop().unwrap() else {
        return Err("This PostgreSQL interface is read-only".into());
    };
    let cat = Catalog::read(node, db)?;
    let system = match query.body.as_ref() {
        SetExpr::Select(select) => match select.from.first().map(|from| &from.relation) {
            None => true,
            Some(TableFactor::Table { name, .. }) => match parts(name).as_slice() {
                [schema, _] => matches!(schema.as_str(), "pg_catalog" | "information_schema"),
                [table] => table.starts_with("pg_"),
                _ => false,
            },
            _ => false,
        },
        _ => false,
    };
    if system {
        return super::metadata::select(&cat, db, pid, query, params, describe, active);
    }
    let flow = visit_expressions_mut(&mut query, |expr| {
        if let Expr::Value(v) = expr {
            if let Value::Placeholder(p) = &v.value {
                let Some(i) = p
                    .strip_prefix('$')
                    .and_then(|n| n.parse::<usize>().ok())
                    .and_then(|n| n.checked_sub(1))
                else {
                    return ControlFlow::Break("Invalid parameter");
                };
                v.value = match params.get(i) {
                    Some(Some(s)) => Value::SingleQuotedString(s.clone()),
                    Some(None) => Value::Null,
                    None if describe => Value::Number("0".into(), false),
                    None => return ControlFlow::Break("Missing parameter"),
                };
            }
        }
        ControlFlow::Continue(())
    });
    if let ControlFlow::Break(msg) = flow {
        return Err(msg.into());
    }
    let (limit, offset) = match query.limit_clause.take() {
        None => (None, 0),
        Some(LimitClause::LimitOffset {
            limit,
            offset,
            limit_by,
        }) if limit_by.is_empty() => (
            limit.as_ref().map(page).transpose()?,
            offset
                .as_ref()
                .map(|o| page(&o.value))
                .transpose()?
                .unwrap_or(0),
        ),
        _ => return Err("Unsupported pagination".into()),
    };
    let SetExpr::Select(select) = query.body.as_mut() else {
        return Err("Expected SELECT".into());
    };
    let [from] = select.from.as_mut_slice() else {
        return Err("Expected one event source or a binary join".into());
    };
    let join = !from.joins.is_empty();
    let left = relation(&mut from.relation, &cat, join)?;
    let right = if let Some(j) = from.joins.first_mut() {
        Some(relation(&mut j.relation, &cat, true)?)
    } else {
        None
    };
    let fields = left
        .fields
        .iter()
        .map(|(n, _)| n.clone())
        .collect::<Vec<_>>();
    let prepared = data_seeds::language::request(
        &json!({"op":"prepare","text":query.to_string(),"fields":fields}),
    )?;
    let plan = &prepared["plan"];
    let mut columns = vec![];
    let mut keys = vec![];
    let field = |table: &Table, name: &str| -> Result<Type> {
        Ok(table
            .fields
            .iter()
            .find(|(n, _)| n == name)
            .ok_or_else(|| format!("Unknown column {name}"))?
            .1
            .clone())
    };
    let rows = if let Some(right) = right {
        let plan: data_seeds::relational::join::Plan =
            serde_json::from_value(plan["join"].clone())?;
        let tables = [left, right];
        if plan.columns.is_empty() {
            for (table, input) in tables.iter().zip(&plan.inputs) {
                for (name, ty) in &table.fields {
                    columns.push((format!("{}.{name}", input.alias), ty.clone()));
                }
            }
        } else {
            for c in &plan.columns {
                columns.push((c.name.clone(), field(tables[c.side.index()], &c.field)?));
            }
        }
        keys.extend(columns.iter().map(|c| c.0.clone()));
        let take = limit
            .map(|n| n.checked_add(offset).ok_or("Join page exceeds range"))
            .transpose()?;
        if describe {
            Rows::Describe
        } else {
            Rows::Join(node.join(&left.branches, 0, u64::MAX, &plan, take)?)
        }
    } else if !plan["relational"]["groupBy"].is_null() {
        for s in plan["relational"]["selection"]
            .as_array()
            .ok_or("Missing aggregate selection")?
        {
            let ty = match s["function"].as_str() {
                Some("count") => Type::INT8,
                Some("avg") => Type::FLOAT8,
                op => {
                    let ty = field(left, s["field"].as_str().ok_or("Missing aggregate field")?)?;
                    if op == Some("sum") {
                        if matches!(ty, Type::FLOAT4 | Type::FLOAT8) {
                            Type::FLOAT8
                        } else {
                            Type::NUMERIC
                        }
                    } else {
                        ty
                    }
                }
            };
            columns.push((
                s["alias"].as_str().ok_or("Missing result name")?.to_owned(),
                ty,
            ));
        }
        if describe {
            Rows::Describe
        } else {
            Rows::Aggregate {
                branches: left.branches.clone(),
                plan: plan.clone(),
                offset,
                limit,
                cancel: Arc::default(),
            }
        }
    } else {
        let mut request = json!({"offset":offset});
        if let Some(n) = limit {
            request["limit"] = json!(n);
        }
        let aliases = crate::sql::rows(plan, &mut request)?;
        if aliases.is_empty() {
            columns = left.fields.clone();
            keys = fields;
            request["select"] = json!(keys);
        } else {
            for ((key, alias), selection) in aliases
                .into_iter()
                .zip(plan["relational"]["selection"].as_array().unwrap())
            {
                if !selection["cast"].is_null() {
                    return Err(
                        "CAST result descriptors are not yet supported over PostgreSQL".into(),
                    );
                }
                columns.push((alias, field(left, &key)?));
                keys.push(key);
            }
        }
        if describe {
            Rows::Describe
        } else {
            Rows::Scan(
                node.query_many(
                    &left.branches,
                    0,
                    u64::MAX,
                    data_seeds::request::query(&request)?,
                )?
                .0,
            )
        }
    };
    Ok(Output {
        columns,
        keys,
        rows,
        skip: if join { offset } else { 0 },
    })
}
