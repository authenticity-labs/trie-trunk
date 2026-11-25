use super::{
    catalog::Catalog,
    query::{Output, Rows},
    *,
};
use rusqlite::types::ValueRef;
use sqlparser::ast::*;
use std::ops::ControlFlow;

// Translate PostgreSQL catalog syntax at the AST boundary. Values stay bound.
#[derive(Default)]
struct Metadata {
    // PostgreSQL exposes a scalar set-returning function's alias as its value.
    subscript_aliases: Vec<Vec<Ident>>,
}

fn subscript_source(f: &TableFactor) -> Option<(&TableAlias, &TableFunctionArgs)> {
    let TableFactor::Table {
        name,
        args: Some(args),
        alias: Some(alias),
        ..
    } = f
    else {
        return None;
    };
    if !alias.columns.is_empty() || name.to_string().to_lowercase() != "generate_subscripts" {
        return None;
    }
    let [
        FunctionArg::Unnamed(FunctionArgExpr::Expr(_)),
        FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Value(dim))),
    ] = args.args.as_slice()
    else {
        return None;
    };
    (dim.value == Value::Number("1".into(), false)).then_some((alias, args))
}

fn integer(n: &str) -> Expr {
    Expr::Value(Value::Number(n.into(), false).into())
}

fn binary(left: Expr, op: BinaryOperator, right: Expr) -> Expr {
    Expr::Nested(Box::new(Expr::BinaryOp {
        left: Box::new(left),
        op,
        right: Box::new(right),
    }))
}
impl VisitorMut for Metadata {
    type Break = String;
    fn pre_visit_query(&mut self, q: &mut Query) -> ControlFlow<String> {
        let mut aliases = Vec::new();
        if let SetExpr::Select(select) = q.body.as_ref() {
            for from in &select.from {
                for f in
                    std::iter::once(&from.relation).chain(from.joins.iter().map(|j| &j.relation))
                {
                    if let Some((alias, _)) = subscript_source(f) {
                        aliases.push(alias.name.clone());
                    }
                }
            }
        }
        self.subscript_aliases.push(aliases);
        ControlFlow::Continue(())
    }

    fn post_visit_query(&mut self, _: &mut Query) -> ControlFlow<String> {
        self.subscript_aliases.pop();
        ControlFlow::Continue(())
    }

    fn post_visit_table_factor(&mut self, f: &mut TableFactor) -> ControlFlow<String> {
        if subscript_source(f).is_some() {
            if let TableFactor::Table {
                name,
                args: Some(args),
                ..
            } = f
            {
                *name = ObjectName::from(Ident::new("json_each"));
                args.args.truncate(1);
            }
        }
        ControlFlow::Continue(())
    }

    fn pre_visit_table_factor(&mut self, f: &mut TableFactor) -> ControlFlow<String> {
        if subscript_source(f).is_none()
            && !matches!(
                f,
                TableFactor::Table { args: None, .. } | TableFactor::Derived { .. }
            )
        {
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
                        | "pg_constraint"
                        | "generate_subscripts"
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
            Expr::Identifier(id)
                if self
                    .subscript_aliases
                    .last()
                    .is_some_and(|aliases| aliases.contains(id)) =>
            {
                let key = Expr::CompoundIdentifier(vec![id.clone(), Ident::new("key")]);
                *e = binary(key, BinaryOperator::Plus, integer("1"));
            }
            Expr::CompoundFieldAccess { root, access_chain }
                if matches!(
                    access_chain.last(),
                    Some(AccessExpr::Subscript(Subscript::Index { .. }))
                ) =>
            {
                let Some(AccessExpr::Subscript(Subscript::Index { index })) = access_chain.pop()
                else {
                    unreachable!()
                };
                let array = if access_chain.is_empty() {
                    *root.clone()
                } else {
                    Expr::CompoundFieldAccess {
                        root: root.clone(),
                        access_chain: std::mem::take(access_chain),
                    }
                };
                *e = binary(
                    array,
                    BinaryOperator::LongArrow,
                    binary(index, BinaryOperator::Minus, integer("1")),
                );
            }
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
    if let ControlFlow::Break(msg) = VisitMut::visit(&mut q, &mut Metadata::default()) {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn constraint_keys_follow_postgres_ordinals() {
        let db = rusqlite::Connection::open_in_memory().unwrap();
        db.execute_batch(include_str!("catalog.sql")).unwrap();
        let mut sql = sqlparser::parser::Parser::parse_sql(
            &sqlparser::dialect::PostgreSqlDialect {},
            "SELECT pos AS ordinal_position, c.conkey[pos], c.confkey[pos]
             FROM pg_catalog.pg_constraint c
             JOIN generate_subscripts(c.conkey, 1) pos ON true
             WHERE c.conname = $1 ORDER BY pos",
        )
        .unwrap()
        .remove(0);
        assert!(VisitMut::visit(&mut sql, &mut Metadata::default()).is_continue());
        let sql = sql.to_string();
        let read = || {
            let mut statement = db.prepare(&sql).unwrap();
            statement
                .query_map(["fk"], |row| {
                    Ok((
                        row.get::<_, i64>(0)?,
                        row.get::<_, i64>(1)?,
                        row.get::<_, i64>(2)?,
                    ))
                })
                .unwrap()
                .collect::<rusqlite::Result<Vec<_>>>()
                .unwrap()
        };
        assert!(read().is_empty());
        db.execute(
            "INSERT INTO pg_constraint(conname, conkey, confkey) VALUES ('fk','[4,2]','[3,8]')",
            [],
        )
        .unwrap();
        assert_eq!(read(), [(1, 4, 3), (2, 2, 8)]);
    }
}
