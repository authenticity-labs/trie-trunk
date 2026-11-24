//! A base annotation identifies the storage family of a registered Definition.
use crate::Error;
use pil_codec::authoring_language::language::{self as lang, Field};
use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum Base {
    Object,
    Event,
    Log,
}
impl Base {
    pub fn tag(self) -> &'static str {
        match self {
            Self::Object => "@object",
            Self::Event => "@event",
            Self::Log => "@log",
        }
    }
    pub fn id_bytes(self) -> u16 {
        if self == Self::Object { 16 } else { 10 }
    }
    pub fn read(fields: &[Field]) -> Result<Option<Self>, Error> {
        let mut found = None;
        for f in fields {
            for a in &f.annotations {
                let base = match a.as_str() {
                    tag if tag == "@id" || tag.starts_with("@id(") =>
                        return Err("Choose @event, @log, or @object on the identity field".into()),
                    "@object" => Self::Object,
                    "@event" => Self::Event,
                    "@log" => Self::Log,
                    _ => continue,
                };
                if found.replace(base).is_some() {
                    return Err("Choose one base annotation on one identity field".into());
                }
                if f.base_kind() != [lang::BYTES, 0] || f.size != base.id_bytes() {
                    return Err(format!(
                        "{} requires a {}-byte identity",
                        base.tag(),
                        base.id_bytes()
                    ));
                }
            }
        }
        let Some(base) = found else {
            return Ok(None);
        };
        if base != Self::Object {
            let mut times = fields.iter().filter(|f| {
                f.annotations.iter().any(|s| {
                    matches!(
                        s.as_str(),
                        "@time" | "@time(ms)" | "@time(us)" | "@time(ns)"
                    )
                })
            });
            if !times
                .next()
                .is_some_and(|f| f.base_kind() == [lang::UINT, 0] && (6..=16).contains(&f.size))
                || times.next().is_some()
            {
                return Err(
                    "Event and Log require one unsigned @time field of at least six bytes".into(),
                );
            }
        }
        Ok(Some(base))
    }
}
