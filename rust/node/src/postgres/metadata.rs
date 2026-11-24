use super::{
    catalog::Catalog,
    query::{Output, Rows},
    *,
};
use rusqlite::types::ValueRef;
use sqlparser::ast::*;
use std::ops::ControlFlow;

// Translate PostgreSQL catalog syntax at the AST boundary. Values stay bound.
struct Metadata;
impl VisitorMut for Metadata {
    type Break = String;
    fn pre_visit_table_factor(&mut self, f: &mut TableFactor) -> ControlFlow<String> {
        if !matches!(
            f,
            TableFactor::Table { args: None, .. } | TableFactor::Derived { .. }
        ) {
            return ControlFlow::Break("Unsupported catalog source".into());
        }
        ControlFlow::Continue(())
    }
    fn pre_visit_relation(&mut self, n: &mut ObjectName) -> ControlFlow<String> {
        if n.0.len() == 2
            && n.0[0].as_ident().is_some_and(|i| {
                matches!(
                    i.value.to_lowercase().as_str(),
                    "pg_catalog" | "information_schema"
                )
            })
        {
            n.0.remove(0);
        }
        if n.0.len() != 1
            || !n.0[0].as_ident().is_some_and(|i| {
                matches!(
                    i.value.to_lowercase().as_str(),
                    "pg_class"
                        | "pg_database"
                        | "pg_type"
                        | "pg_namespace"
                        | "pg_attribute"
                        | "pg_inherits"
                        | "pg_index"
                        | "pg_proc"
                        | "pg_enum"
                        | "tables"
                        | "columns"
                        | "schemata"
                        | "views"
                        | "parameters"
                        | "routines"
                )
            })
        {
            return ControlFlow::Break("Unknown catalog relation".into());
        }
        ControlFlow::Continue(())
    }
    fn post_visit_expr(&mut self, e: &mut Expr) -> ControlFlow<String> {
        match e {
            Expr::AnyOp {
                left,
                compare_op: BinaryOperator::Eq,
                right,
                ..
            } if matches!(right.as_ref(), Expr::CompoundIdentifier(ids) if ids.last().is_some_and(|id| id.value == "indkey")) =>
            {
                // The read-only catalog exposes no primary-key indexes.
                *e = Expr::BinaryOp {
                    left: left.clone(),
                    op: BinaryOperator::Eq,
                    right: right.clone(),
                };
            }
            Expr::Cast {
                kind,
                expr,
                data_type,
                ..
            } => {
                let ty = data_type.to_string().to_lowercase();
                if matches!(ty.as_str(), "regclass" | "regnamespace") {
                    // The expression is already parsed. Preserve its quoting when wrapping it.
                    let expr_sql = format!("{ty}({expr})");
                    match sqlparser::parser::Parser::new(&sqlparser::dialect::SQLiteDialect {})
                        .try_with_sql(&expr_sql)
                        .and_then(|mut p| p.parse_expr())
                    {
                        Ok(v) => *e = v,
                        Err(err) => return ControlFlow::Break(err.to_string()),
                    }
                } else {
                    *kind = CastKind::Cast;
                }
            }
            Expr::Function(f) => {
                if f.name.0.len() == 2
                    && f.name.0[0]
                        .as_ident()
                        .is_some_and(|i| i.value.eq_ignore_ascii_case("pg_catalog"))
                {
                    f.name.0.remove(0);
                }
                if !matches!(
                    f.name.to_string().to_lowercase().as_str(),
                    "current_schema"
                        | "version"
                        | "current_database"
                        | "pg_backend_pid"
                        | "pg_cancel_backend"
                        | "format_type"
                        | "quote_ident"
                        | "col_description"
                        | "format"
                        | "coalesce"
                        | "nullif"
                        | "count"
                        | "lower"
                        | "upper"
                ) {
                    return ControlFlow::Break(format!("Unsupported catalog function {}", f.name));
                }
            }
            _ => {}
        }
        ControlFlow::Continue(())
    }
}

pub fn select(
    cat: &Catalog,
    db: &str,
    pid: i32,
    mut q: Box<Query>,
    params: &[Option<String>],
    describe: bool,
    active: &Arc<Active>,
) -> Result<Output> {
    if let ControlFlow::Break(msg) = VisitMut::visit(&mut q, &mut Metadata) {
        return Err(msg.into());
    }
    if let SetExpr::Select(s) = q.body.as_mut() {
        for item in &mut s.projection {
            if let SelectItem::UnnamedExpr(expr @ Expr::Function(_)) = item {
                let Expr::Function(f) = expr else {
                    unreachable!()
                };
                let alias = Ident::new(f.name.to_string().to_lowercase());
                *item = SelectItem::ExprWithAlias {
                    expr: expr.clone(),
                    alias,
                };
            }
        }
    }
    let conn = cat.metadata(db, pid)?;
    let mut names = std::collections::HashMap::new();
    let mut spaces = std::collections::HashMap::new();
    for ((schema, name), t) in &cat.tables {
        let id = i64::from(*t.branches.iter().min().unwrap()) + 65536;
        names.insert(
            format!(
                "{}.{}",
                super::catalog::quote(schema),
                super::catalog::quote(name)
            ),
            id,
        );
        names.insert(format!("{schema}.{name}"), id);
        let next = spaces.len() as i64 + 1;
        spaces.entry(schema.clone()).or_insert(next);
    }
    let flags = rusqlite::functions::FunctionFlags::SQLITE_UTF8;
    let active = active.clone();
    conn.create_scalar_function("pg_cancel_backend", 1, flags, move |ctx| {
        Ok(active.stop(ctx.get::<i32>(0)?, None))
    })?;
    conn.create_scalar_function("regclass", 1, flags, move |ctx| {
        Ok(names.get(&ctx.get::<String>(0)?).copied())
    })?;
    conn.create_scalar_function("regnamespace", 1, flags, move |ctx| {
        let n = ctx.get::<i64>(0)?;
        Ok(spaces
            .iter()
            .find(|(_, v)| **v == n)
            .map(|(s, _)| super::catalog::quote(s)))
    })?;
    let mut boolean_slots = std::collections::HashSet::new();
    let _: ControlFlow<()> = visit_expressions(&q, |expr| {
        if let Expr::BinaryOp { left, right, .. } = expr {
            for (column, parameter) in [(left, right), (right, left)] {
                if matches!(column.as_ref(), Expr::Identifier(i) if i.value.eq_ignore_ascii_case("datistemplate"))
                {
                    if let Expr::Value(v) = parameter.as_ref() {
                        if let Value::Placeholder(name) = &v.value {
                            boolean_slots.insert(name.clone());
                        }
                    }
                }
            }
        }
        ControlFlow::Continue(())
    });
    let mut stmt = conn.prepare(&q.to_string())?;
    if !stmt.readonly() {
        return Err("Catalog is read-only".into());
    }
    let columns = stmt
        .column_names()
        .into_iter()
        .map(|name| {
            let ty = match name {
                "pid" | "ordinal_position" | "typeid" => Type::INT4,
                "total" | "count" => Type::INT8,
                "pg_cancel_backend" => Type::BOOL,
                _ => Type::TEXT,
            };
            (name.to_owned(), ty)
        })
        .collect::<Vec<_>>();
    let mut rows = Vec::new();
    if !describe {
        for (i, value) in params.iter().enumerate() {
            let slot = stmt
                .parameter_index(&format!("${}", i + 1))?
                .ok_or("Unexpected catalog parameter")?;
            if boolean_slots.contains(&format!("${}", i + 1)) {
                let flag = value
                    .as_deref()
                    .map(|s| match s {
                        "false" | "f" | "0" => Ok(false),
                        "true" | "t" | "1" => Ok(true),
                        _ => Err("Invalid Boolean"),
                    })
                    .transpose()?;
                stmt.raw_bind_parameter(slot, flag)?;
            } else {
                stmt.raw_bind_parameter(slot, value)?;
            }
        }
        let mut cursor = stmt.raw_query();
        let mut bytes = 0;
        while let Some(row) = cursor.next()? {
            let row = (0..columns.len())
                .map(|i| -> Result<_> {
                    Ok(match row.get_ref(i)? {
                        ValueRef::Null => None,
                        ValueRef::Integer(v) if columns[i].1 == Type::BOOL => {
                            Some((v != 0).to_string())
                        }
                        ValueRef::Integer(v) => Some(v.to_string()),
                        ValueRef::Real(v) => Some(v.to_string()),
                        ValueRef::Text(v) => Some(std::str::from_utf8(v)?.to_owned()),
                        _ => return Err("Unexpected binary catalog value".into()),
                    })
                })
                .collect::<Result<Vec<_>>>()?;
            bytes += row
                .iter()
                .map(|v| 32 + v.as_ref().map_or(0, String::len))
                .sum::<usize>();
            if bytes > 4 << 20 {
                return Err("Catalog response exceeds 4 MiB".into());
            }
            rows.push(row);
        }
    }
    Ok(Output {
        columns,
        keys: vec![],
        rows: Rows::Catalog(rows.into_iter()),
        skip: 0,
    })
}
