//! Lower a prepared SQL projection into the existing Seed row request.
use crate::Result;
use serde_json::{Value, json};

pub(crate) fn rows(plan: &Value, query: &mut Value) -> Result<Vec<(String, String)>> {
    if let Some(predicate) = plan.get("predicate") {
        query["where"] = data_seeds::request::language_predicate(predicate)?;
    }
    if let Some(order) = plan.get("orderBy") {
        query["orderBy"] = order.clone();
    }
    let selection = plan["relational"]["selection"]
        .as_array()
        .ok_or("Missing SQL selection")?;
    if !selection.is_empty() {
        query["select"] = json!(
            selection
                .iter()
                .map(|s| s["field"].clone())
                .collect::<Vec<_>>()
        );
        let casts = selection
            .iter()
            .filter(|s| !s["cast"].is_null())
            .cloned()
            .collect::<Vec<_>>();
        if !casts.is_empty() {
            query["casts"] = json!(casts);
        }
        return selection
            .iter()
            .map(|s| {
                Ok((
                    s[if s["cast"].is_null() {
                        "field"
                    } else {
                        "alias"
                    }]
                    .as_str()
                    .ok_or("Missing SQL field")?
                    .into(),
                    s["alias"].as_str().ok_or("Missing SQL alias")?.into(),
                ))
            })
            .collect::<Result<_>>();
    }
    Ok(Vec::new())
}
