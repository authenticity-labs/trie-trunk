//! Apply a base to the root of an authored RX. Imported layouts stay exact.
use crate::{Error, doc, error};
use pil_codec::authoring_language::language::{self as lang, Field, TypeDescriptor};
use serde_json::{Value, json};
use trie_trunk_taxonomy::{base::Base, registration::attribute_label};

pub fn bind(
    bytes: &[u8],
    base: Base,
    authored: bool,
    id: &str,
    time: &str,
) -> Result<Vec<u8>, Error> {
    let doc = doc::read(bytes)?;
    let mut members = doc.definitions().to_vec();
    let fields = &mut members
        .first_mut()
        .ok_or_else(|| error("Empty Definition"))?
        .fields;
    if let Some(existing) = Base::read(fields).map_err(error)? {
        if existing != base {
            return Err(error(
                "The selected base conflicts with the RX identity annotation",
            ));
        }
        return Ok(bytes.to_vec());
    }
    fn bind_field(
        fields: &mut Vec<Field>,
        name: &str,
        tag: &str,
        kind: u8,
        size: u16,
        authored: bool,
    ) -> Result<usize, Error> {
        let position = fields
            .iter()
            .position(|f| {
                f.annotations
                    .iter()
                    .any(|a| a == tag || (tag == "@time" && a.starts_with("@time(")))
            })
            .or_else(|| fields.iter().position(|f| f.name == name));
        let index = match position {
            Some(i) => i,
            None if authored && !name.trim().is_empty() => {
                fields.push(Field::scalar(name, kind));
                fields.len() - 1
            }
            _ => return Err(error(format!("Choose the {tag} field in this RX"))),
        };
        let f = &mut fields[index];
        if authored
            && !(tag == "@time" && f.base_kind() == [lang::UINT, 0] && (6..=16).contains(&f.size))
        {
            f.descriptor = TypeDescriptor::scalar(kind);
            f.size = if tag == "@time"
                && f.annotations
                    .iter()
                    .any(|a| matches!(a.as_str(), "@time(ns)" | "@time(us)"))
            {
                8
            } else {
                size
            };
        }
        if !f
            .annotations
            .iter()
            .any(|a| a == tag || (tag == "@time" && a.starts_with("@time(")))
        {
            f.annotations.push(tag.into());
        }
        Ok(index)
    }
    let i = bind_field(fields, id, base.tag(), lang::BYTES, base.id_bytes(), authored)?;
    if base != Base::Object {
        let j = bind_field(fields, time, "@time", lang::UINT, 6, authored)?;
        if i == j {
            return Err(error("Choose distinct identity and timestamp fields"));
        }
    }
    Base::read(fields).map_err(error)?;
    for member in &mut members {
        member.header.rx_hash.clear();
    }
    Ok(doc::Document::from_definitions(members)
        .sign()?
        .bytes()?
        .to_vec())
}

pub fn model(document: doc::Document, member: usize) -> Result<Value, Error> {
    let header = document
        .definitions()
        .get(member)
        .ok_or_else(|| error("Unknown Definition revision"))?
        .header
        .clone();
    let selected = document.retain_members(&[member])?;
    let root = selected.member(
        &header.namespace,
        &header.rx_name,
        header.rx_major,
        header.rx_minor,
    )?;
    let members=selected.definitions().iter().map(|m|{
        let fields=m.fields.iter().map(|f|json!({"name":f.name,"type":attribute_label(f),"annotations":f.annotations,"descriptor":f.descriptor})).collect::<Vec<_>>();
        json!({"header":m.header,"fields":fields})
    }).collect::<Vec<_>>();
    Ok(json!({"root":root,"members":members}))
}
