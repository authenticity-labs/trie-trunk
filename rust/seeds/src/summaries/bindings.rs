//! Seed-owned source names and category membership. One descriptor per Pack
//! plan; PIL only sees fixed arrays and is unaware of these analytic bindings.
use super::{SummaryClass, annotated::number_type};
use crate::{Cell, Document, Error, Field, SeedDefinition, doc, error};
use ciborium::Value as Data;
use std::sync::{Arc, OnceLock};

pub(super) const HEADER: &str = "summary.bindings";
fn schema() -> Result<&'static pil_codec::api::Prescription, Error> {
    static RX: OnceLock<Result<pil_codec::api::Prescription, String>> = OnceLock::new();
    RX.get_or_init(|| {
        (|| -> Result<_, Error> {
            doc::new("summary-bindings")
                .field(Field::new("fields").array_object("binding"))
                .definition("binding")
                .field(Field::new("name").r#type("tstr"))
                .field(Field::new("kind").r#type("tstr"))
                .field(Field::new("size").r#type("uint").size(1))
                .field(Field::new("values").array("any"))
                .field(Field::new("annotations").array("tstr"))
                .select(0)
                .sign()?
                .prescribe()
        })()
        .map_err(|e| e.to_string())
    })
    .as_ref()
    .map_err(error)
}
pub(super) fn write(source: &SeedDefinition) -> Result<Vec<u8>, Error> {
    let mut fields = Vec::new();
    for field in source.summary_fields() {
        let (kind, size) = match field.class {
            SummaryClass::Numeric | SummaryClass::Temporal => number_type(
                source
                    .prescription()
                    .native_scalar_kind(field.ordinal)
                    .ok_or_else(|| error("missing summary scalar"))?,
            )?,
            SummaryClass::Boolean => ("bool", 0),
            _ => match field.values.first() {
                Some(Data::Text(_)) => ("tstr", 0),
                Some(Data::Bool(_)) => ("bool", 0),
                Some(_)
                    if field
                        .values
                        .iter()
                        .any(|v| v.as_integer().is_some_and(|n| i128::from(n) < 0)) =>
                {
                    ("int", 0)
                }
                Some(_) => ("uint", 0),
                None => return Err(error("missing category vocabulary")),
            },
        };
        let mut annotations = super::definitions::semantic_annotations(source, field.ordinal);
        annotations.push(format!(
            "#summ({})",
            super::definitions::class_name(field.class)
        ));
        fields.push(Data::Map(vec![
            ("name".into(), field.name.clone().into()),
            ("kind".into(), kind.into()),
            ("size".into(), size.into()),
            ("values".into(), Data::Array(field.values.clone())),
            (
                "annotations".into(),
                Data::Array(annotations.into_iter().map(Data::Text).collect()),
            ),
        ]));
    }
    let mut pill = schema()?.dispense();
    pill.set_array("fields", fields)?.cap()?;
    Ok(pill.bytes()?.to_vec())
}
pub(super) fn read(bytes: &[u8]) -> Result<Arc<SeedDefinition>, Error> {
    let mut decoder = schema()?.decoder();
    let row = decoder.read(bytes)?;
    let Cell::Value(Data::Array(fields)) = crate::value::own(row.native_value(0)?)? else {
        return Err(error("invalid summary bindings"));
    };
    let mut source: Document = doc::new("summary-inputs");
    let mut names = std::collections::HashSet::new();
    for field in fields {
        let Data::Map(fields) = field else {
            return Err(error("invalid summary binding"));
        };
        let get = |name: &str| {
            fields
                .iter()
                .find(|(k, _)| k.as_text() == Some(name))
                .map(|(_, v)| v)
                .ok_or_else(|| error("incomplete summary binding"))
        };
        let text = |name| {
            get(name)?
                .as_text()
                .ok_or_else(|| error("invalid summary binding text"))
        };
        let name = text("name")?;
        if !names.insert(name.to_owned()) {
            return Err(error("duplicate summary field binding"));
        }
        let size = u16::try_from(i128::from(
            get("size")?
                .as_integer()
                .ok_or_else(|| error("invalid summary width"))?,
        ))
        .map_err(|_| Error::Overflow)?;
        let values = get("values")?
            .as_array()
            .ok_or_else(|| error("invalid category binding"))?;
        let annotations = get("annotations")?
            .as_array()
            .ok_or_else(|| error("invalid summary annotations"))?
            .iter()
            .map(|v| {
                v.as_text()
                    .map(str::to_owned)
                    .ok_or_else(|| error("invalid summary annotation"))
            })
            .collect::<Result<Vec<_>, _>>()?;
        let field = if values.is_empty() {
            Field::new(name).r#type(text("kind")?).size(size)
        } else {
            Field::new(name).closed_enum(
                text("kind")?,
                values
                    .iter()
                    .cloned()
                    .map(pil_codec::json_view::data_json)
                    .collect::<Result<Vec<_>, _>>()
                    .map_err(|e| error(e.to_string()))?,
            )
        };
        source = source.field(field.annotations(annotations));
    }
    SeedDefinition::prescribe(&source.sign()?)
}
