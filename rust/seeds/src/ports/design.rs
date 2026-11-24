//! Definition-only authoring. DOC owns inference, referenced members and RX encoding.
use crate::{Error, doc, error};
use pil_codec::authoring_language::language::{self as lang, TypeDescriptor};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json, value::RawValue};

#[derive(Deserialize)]
#[serde(tag = "action", rename_all = "lowercase", deny_unknown_fields)]
pub enum Design {
    Bind { bytes:Vec<u8>, base:trie_trunk_taxonomy::base::Base, authored:bool, id:String,time:String },
    Revise { bytes: Vec<u8>, changes: Vec<FieldChange> },
    Inspect {bytes:Vec<u8>, member:usize},
    Sample { source: String },
    Build { name: String, members: Vec<Member> },
}

/// Only explicitly edited properties cross the presentation boundary.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FieldChange {
    member: usize,
    field: usize,
    name: String,
    size: u16,
}

#[derive(Serialize, Deserialize)]
pub struct Member {
    // Relative to the name the developer gives the root when registering.
    path: String,
    fields: Vec<Field>,
}

#[derive(Serialize, Deserialize)]
struct Field {
    name: String,
    kind: String,
    size: u16,
    #[serde(default)]
    annotations: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    shape: Option<TypeDescriptor>,
    #[serde(skip_serializing_if = "Option::is_none")]
    target: Option<u8>,
    #[serde(skip_serializing_if = "Option::is_none")]
    element: Option<String>,
}

impl Design {
    pub fn run(self) -> Result<Value, Error> {
        match self {
            Self::Bind {bytes,base,authored,id,time} => Ok(json!(super::model::bind(&bytes,base,authored,&id,&time)?)),
            Self::Inspect {bytes,member} => super::model::model(doc::read(bytes.as_slice())?,member),
            Self::Revise { bytes, changes } => {
                let mut document = doc::read(bytes.as_slice())?;
                for change in changes {
                    let mut field = document.definitions().get(change.member)
                        .and_then(|m| m.fields.get(change.field)).cloned()
                        .ok_or_else(|| error("Unknown draft field"))?;
                    if change.name.trim().is_empty() {
                        return Err(error("Enter a field name"));
                    }
                    field.name = change.name;
                    field.size = change.size;
                    document = document.select(change.member)
                        .replace_field(change.field, doc::Field::from_field(field));
                }
                let signed = document.sign()?;
                signed.validate()?;
                Ok(json!(signed.bytes()?))
            }
            Self::Sample { source } => sample(&source),
            Self::Build { name, members } => {
                if name.trim().is_empty()
                    || members
                        .first()
                        .is_none_or(|m| !m.path.is_empty() || m.fields.is_empty())
                {
                    return Err(error("Enter a type name and at least one field"));
                }
                let mut document = doc::new(&name);
                for (index, member) in members.into_iter().enumerate() {
                    if index > 0 {
                        document = document.definition(format!("{name}{}", member.path));
                    }
                    for f in member.fields {
                        let field = if let Some(descriptor) = f.shape {
                            doc::Field::from_field(lang::Field {
                                name: f.name,
                                descriptor,
                                annotations: vec![],
                            })
                        } else {
                            let builder = doc::Field::new(f.name);
                            match f.kind.as_str() {
                                "object" => builder.object(
                                    f.target
                                        .ok_or_else(|| error("Choose a nested Definition"))?,
                                ),
                                "array" if f.target.is_some() => {
                                    builder.array_object(f.target.unwrap())
                                }
                                "array" => builder.array(f.element.as_deref().unwrap_or("any")),
                                "map" => builder.map("tstr", f.element.as_deref().unwrap_or("any")),
                                _ => builder.r#type(&f.kind),
                            }
                        };
                        document = document.field(field.size(f.size).annotations(f.annotations));
                    }
                }
                let signed = document.retain_members(&[0])?;
                signed.validate()?;
                Ok(json!(signed.bytes()?))
            }
        }
    }
}

fn sample(source: &str) -> Result<Value, Error> {
    if source.len() > 1 << 20 {
        return Err(error("Choose a sample of 1 MiB or less"));
    }
    let parse = |e: serde_json::Error| error(e.to_string());
    let mut lines = String::new();
    let mut append = |record: &RawValue| -> Result<(), Error> {
        if !record.get().starts_with('{') {
            return Err(error("Each sample record must be a JSON object"));
        }
        // Preserve number lexemes and duplicate keys for DOC validation.
        for line in record.get().lines() {
            lines.push_str(line.trim());
        }
        lines.push('\n');
        Ok(())
    };
    if source.trim_start().starts_with('[') {
        for item in serde_json::from_str::<Vec<&RawValue>>(source).map_err(parse)? {
            append(item)?;
        }
    } else {
        for item in serde_json::Deserializer::from_str(source).into_iter::<&RawValue>() {
            append(item.map_err(parse)?)?;
        }
    }
    let candidate = doc::source::Candidate::read(lines.as_bytes(), Default::default())?;
    let document = candidate.document("draft")?;
    let members = document
        .definitions()
        .iter()
        .map(|member| Member {
            path: member
                .header
                .rx_name
                .strip_prefix("draft")
                .unwrap_or(&member.header.rx_name)
                .into(),
            fields: member
                .fields
                .iter()
                .map(|f| {
                    let (kind, composite) = match f.base_kind() {
                        [lang::UINT, 0] => ("uint", false),
                        [lang::INT, 0] => ("int", false),
                        [lang::FLOAT, 0] => ("float", false),
                        [lang::BOOL, 0] => ("bool", false),
                        [lang::TSTR, 0] => ("tstr", false),
                        [lang::BYTES, 0] => ("bytes", false),
                        [lang::ANY, 0] => ("any", false),
                        [lang::NULL, 0] => ("null", false),
                        [lang::OBJECT, _] => ("object", true),
                        [lang::ARRAY, _] => ("array", true),
                        _ => ("map", true),
                    };
                    Field {
                        name: f.name.clone(),
                        kind: kind.into(),
                        size: f.size,
                        annotations: f.annotations.clone(),
                        shape: composite.then(|| f.descriptor.clone()),
                        target: None,
                        element: None,
                    }
                })
                .collect(),
        })
        .collect::<Vec<_>>();
    Ok(json!({"members":members, "records":candidate.records}))
}
