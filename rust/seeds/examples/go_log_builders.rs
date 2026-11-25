//! Generate the Go SDK's built-in fluent APIs from the same RX and option names as the encoder.
use data_seeds::{
    Document, SeedDefinition, doc,
    registration::query_events,
    seed::log::{self, Style},
};
use pil_codec::authoring_language::language::{self as rx, Field};
use std::{collections::HashSet, error::Error, fmt::Write, fs};

type Result<T> = std::result::Result<T, Box<dyn Error>>;

fn ident(name: &str) -> String {
    name.split(|c: char| !c.is_ascii_alphanumeric())
        .filter(|word| !word.is_empty())
        .map(|word| match word {
            "http" => "HTTP".to_owned(),
            "id" => "ID".to_owned(),
            _ => {
                let mut word = word.to_owned();
                word[..1].make_ascii_uppercase();
                if word.ends_with("Id") {
                    word.replace_range(word.len() - 2.., "ID");
                }
                word
            }
        })
        .collect()
}

fn scalar(f: &Field) -> Result<String> {
    let ty = match (f.base_kind(), f.size) {
        ([rx::UINT, 0], 1) => "uint8",
        ([rx::UINT, 0], 2) => "uint16",
        ([rx::UINT, 0], 3..=4) => "uint32",
        ([rx::UINT, 0], 5..=8) => "uint64",
        ([rx::FLOAT, 0], 4) => "float32",
        ([rx::FLOAT, 0], 8) => "float64",
        ([rx::BOOL, 0], _) => "bool",
        ([rx::TSTR, 0], _) => "string",
        ([rx::BYTES, 0], 0) => "[]byte",
        ([rx::BYTES, 0], n) => return Ok(format!("[{n}]byte")),
        ([rx::ANY, 0], _) => "any",
        _ => {
            return Err(format!(
                "No Go setter type for {} ({:?}, {} bytes)",
                f.name,
                f.base_kind(),
                f.size
            )
            .into());
        }
    };
    Ok(ty.into())
}

fn generate(out: &mut String, doc: Document) -> Result<()> {
    let def = SeedDefinition::prescribe(&doc)?;
    // prescribe() selects the root member of the RX bundle.
    let root = &doc.definitions()[0];
    let kind = &root.header.rx_name;
    let namespace = &root.header.namespace;
    let name = ident(kind);
    let mut emitted = HashSet::new();
    for (prefix, style, selector) in [
        ("", Style::Trunk, "TrieTrunk"),
        ("OTel", Style::OpenTelemetry, "OpenTelemetry"),
    ] {
        let api = format!("{prefix}{name}");
        let event = format!("{api}Event");
        let logger = def.logger(style)?;
        let constructor = if style == Style::Trunk {
            let factory = format!("{kind}Factory");
            writeln!(out, "\ntype {factory} func() (*TypedLog[*{event}], error)")?;
            format!("var {name} {factory} = func")
        } else {
            writeln!(
                out,
                "\n// OpenTelemetry opens the OpenTelemetry builder for {namespace}.{kind}.\nfunc ({kind}Factory) OpenTelemetry() (*TypedLog[*{event}], error) {{ return {api}() }}"
            )?;
            format!("func {api}")
        };
        writeln!(
            out,
            "\n// {api} opens the {selector} builder for {namespace}.{kind}.\n{constructor}() (*TypedLog[*{event}], error) {{\n return typedLog(\"{namespace}.{kind}\", {selector}, func(r logRecord) *{event} {{ return &{event}{{r}} }})\n}}\ntype {event} struct {{ logRecord }}"
        )?;
        for opt in logger.options() {
            let f = root
                .fields
                .iter()
                .find(|f| f.name == opt.field)
                .ok_or("Missing RX field")?;
            let ty = if f.base_kind() == [rx::ARRAY, rx::OBJECT] {
                let target = f
                    .values
                    .first()
                    .and_then(|v| v.as_u64())
                    .and_then(|i| doc.definitions().get(i as usize))
                    .ok_or("Missing array element Definition")?;
                let nested = format!("{name}{}", ident(&target.header.rx_name));
                if emitted.insert(nested.clone()) {
                    writeln!(
                        out,
                        "\n// {nested} preserves omitted members and explicitly supplied zeroes.\ntype {nested} struct {{"
                    )?;
                    for member in &target.fields {
                        writeln!(
                            out,
                            "{} *{} `cbor:\"{},omitempty\"`",
                            ident(&member.name),
                            scalar(member)?,
                            member.name
                        )?;
                    }
                    writeln!(out, "}}")?;
                }
                format!("...{nested}")
            } else {
                scalar(f)?
            };
            let method = ident(&opt.name);
            let unit = opt
                .unit
                .as_deref()
                .map(|u| format!(" in {u}"))
                .unwrap_or_default();
            writeln!(
                out,
                "\n// {method} sets {}{}.\nfunc (e *{event}) {method}(value {ty}) *{event} {{",
                opt.name, unit
            )?;
            if let Some(element) = ty.strip_prefix("...") {
                // Zero arguments denote an explicitly empty list. A nil Go slice encodes as null.
                writeln!(out, "if value == nil {{ value = []{element}{{}} }}")?;
            }
            writeln!(out, "e.seed.Set({:?}, value)\n return e\n}}", opt.name)?;
        }
        if logger.custom_fields() {
            writeln!(
                out,
                "\n// Field adds an application attribute.\nfunc (e *{event}) Field(name string, value any) *{event} {{\n e.seed.Field(name, value)\n return e\n}}"
            )?;
        }
    }
    Ok(())
}

fn main() -> Result<()> {
    let path = std::env::args()
        .nth(1)
        .ok_or("Expected the Go output path")?;
    let mut out = String::from(
        "// Code generated from the Trunk log Definitions by go generate. DO NOT EDIT.\n\npackage logging\n",
    );
    generate(&mut out, doc::read(log::HTTP)?)?;
    generate(&mut out, query_events::definition()?)?;
    fs::write(path, out)?;
    Ok(())
}
