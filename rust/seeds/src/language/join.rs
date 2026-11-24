use super::*;
use crate::relational::join::{Column, Input, Kind, Plan, Side};

fn ident(i: &Ident) -> String {
    if i.quote_style.is_some() {
        i.value.clone()
    } else {
        i.value.to_lowercase()
    }
}
fn input(t: &TableFactor) -> Result<Input, String> {
    let TableFactor::Table { name, alias, .. } = t else {
        return Err("Expected a registered event source".into());
    };
    let suffix = alias.as_ref().map(|a| format!(" {a}")).unwrap_or_default();
    if t.to_string() != format!("{name}{suffix}")
        || alias
            .as_ref()
            .is_some_and(|a| !a.columns.is_empty() || a.at.is_some())
    {
        return Err("Unsupported join source modifier".into());
    }
    let path: Vec<_> = name
        .0
        .iter()
        .map(|p| p.as_ident().map(ident).ok_or("Expected table name"))
        .collect::<Result<_, _>>()?;
    if path.len() > 4 || path == ["summaries"] {
        return Err("Expected a registered event source".into());
    }
    let alias = alias
        .as_ref()
        .map(|a| ident(&a.name))
        .unwrap_or_else(|| path.last().unwrap().clone());
    Ok(Input {
        path,
        alias,
        filters: vec![],
    })
}
fn field(e: &Expr, inputs: &[Input; 2]) -> Result<(Side, Ident), String> {
    let Expr::CompoundIdentifier(parts) = e else {
        return Err("Qualify join fields with their input alias".into());
    };
    let [table, column] = parts.as_slice() else {
        return Err("Expected alias.field".into());
    };
    let alias = ident(table);
    let side = if alias == inputs[0].alias {
        Side::Left
    } else if alias == inputs[1].alias {
        Side::Right
    } else {
        return Err(format!("Unknown input {alias}"));
    };
    Ok((side, column.clone()))
}
fn terms<'a>(e: &'a Expr, out: &mut Vec<&'a Expr>) {
    match e {
        Expr::Nested(e) => terms(e, out),
        Expr::BinaryOp {
            left,
            op: BinaryOperator::And,
            right,
        } => {
            terms(left, out);
            terms(right, out);
        }
        _ => out.push(e),
    }
}
pub(super) fn parse(s: &Select) -> Result<Plan, String> {
    let source = &s.from[0];
    let [join] = source.joins.as_slice() else {
        return Err("One binary join is supported".into());
    };
    if join.global
        || !matches!(&s.group_by,GroupByExpr::Expressions(v,m) if v.is_empty() && m.is_empty())
    {
        return Err("Unsupported join modifier or grouping".into());
    }
    let (kind, on) = match &join.join_operator {
        JoinOperator::Join(JoinConstraint::On(e)) | JoinOperator::Inner(JoinConstraint::On(e)) => {
            (Kind::Inner, e)
        }
        JoinOperator::Left(JoinConstraint::On(e))
        | JoinOperator::LeftOuter(JoinConstraint::On(e)) => (Kind::Left, e),
        _ => return Err("Use INNER JOIN or LEFT JOIN with ON".into()),
    };
    let mut inputs = [input(&source.relation)?, input(&join.relation)?];
    if inputs[0].alias == inputs[1].alias {
        return Err("Use distinct input aliases".into());
    }
    let mut keys = vec![];
    for (in_on, expr) in [(true, Some(on)), (false, s.selection.as_ref())] {
        let Some(expr) = expr else { continue };
        let mut parts = vec![];
        terms(expr, &mut parts);
        for e in parts {
            let Expr::BinaryOp { left, op, right } = e else {
                return Err("Expected a join comparison".into());
            };
            let (a, col) = field(left, &inputs)?;
            if let Expr::CompoundIdentifier(_) = right.as_ref() {
                let (b, r) = field(right, &inputs)?;
                if !in_on || *op != BinaryOperator::Eq || a == b {
                    return Err("Join keys require equality between the two inputs in ON".into());
                }
                keys.push(if a == Side::Left {
                    [ident(&col), ident(&r)]
                } else {
                    [ident(&r), ident(&col)]
                });
            } else {
                if kind == Kind::Left
                    && ((in_on && a == Side::Left) || (!in_on && a == Side::Right))
                {
                    return Err("LEFT JOIN supports right-input filters in ON and left-input filters in WHERE".into());
                }
                let predicate = Expr::BinaryOp {
                    left: Box::new(Expr::Identifier(col.clone())),
                    op: op.clone(),
                    right: right.clone(),
                };
                let (positive, _) = truth(&predicate, &HashSet::from([ident(&col)]))?;
                inputs[a.index()].filters.push(
                    crate::request::language_predicate(&positive).map_err(|e| e.to_string())?,
                );
            }
        }
    }
    if keys.is_empty() {
        return Err("JOIN requires at least one cross-input equality key".into());
    }
    let mut columns = vec![];
    let mut names = HashSet::new();
    for item in &s.projection {
        if matches!(item,SelectItem::Wildcard(options) if options.to_string().is_empty() && s.projection.len()==1)
        {
            break;
        }
        let (expr, alias) = match item {
            SelectItem::UnnamedExpr(e) => (e, None),
            SelectItem::ExprWithAlias { expr, alias } => (expr, Some(ident(alias))),
            _ => return Err("Unsupported join projection".into()),
        };
        let (side, f) = field(expr, &inputs)?;
        let field = ident(&f);
        let name = alias.unwrap_or_else(|| field.clone());
        if !names.insert(name.clone()) {
            return Err(format!("Duplicate projection {name}; use AS"));
        }
        columns.push(Column { side, field, name });
    }
    Ok(Plan {
        inputs,
        kind,
        keys,
        columns,
    })
}
