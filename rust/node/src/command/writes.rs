//! CLI input/output adapter for the existing registration and ingestion owner.
use crate::{Result, cli::Connection};
use serde_json::json;
use std::{
    fs::File,
    io::{Read, Write},
    path::PathBuf,
};

const HELP: &str = "Usage:
  trunk definitions register --dir PATH|--socket PATH --cache NAME --service NAME --resource NAME --type NAME --file RX|-
  trunk seeds load --dir PATH|--socket PATH --branch ID --file PIL|-
RX and PIL inputs are raw PIL bytes. '-' reads stdin. Seeds load accepts one complete Pill.
  trunk definitions draft --dir PATH --cache NAME --service NAME --resource NAME --type NAME --file RX [--generation TOKEN] [--discard]
  trunk definitions promote --dir PATH --cache NAME --service NAME --resource NAME --type NAME --generation TOKEN
  trunk seeds test --dir PATH --cache NAME --service NAME --resource NAME --type NAME --generation TOKEN --file PIL
Promotion permanently discards this draft’s test data. Registration uses the RX lineage; --split selects an existing configured split. No branch ID is required.
Acknowledgements are JSON on stdout; ingestion acknowledges durable recovery.";

pub(super) fn run(args: &[String], mut input: impl Read, output: &mut impl Write) -> Result<()> {
    if args.iter().any(|s| s == "--help" || s == "-h") {
        writeln!(output, "{HELP}")?;
        return Ok(());
    }
    let command = args.get(1).map(String::as_str);
    let action = match (args.first().map(String::as_str), command) {
        (Some("definitions"), Some("draft")) => Some("save"),
        (Some("definitions"), Some("promote")) => Some("promote"),
        (Some("seeds"), Some("test")) => Some("ingest"),
        _ => None,
    };
    let mut generation = None;
    let mut discard = false;
    let register = action.is_some()
        || matches!(args.get(..2), Some(a) if a[0] == "definitions" && a[1] == "register");
    if !register && !matches!(args.get(..2), Some(a) if a[0] == "seeds" && a[1] == "load") {
        return Err(HELP.into());
    }
    let (mut dir, mut socket, mut file) = (None, None, None);
    let mut target = crate::RegistrationTarget {
        cache: String::new(),
        service: String::new(),
        resource: String::new(),
        resource_type: String::new(),
        branch: None,
        split: String::new(),
        ..Default::default()
    };
    let mut args = args[2..].iter();
    while let Some(flag) = args.next() {
        if flag == "--discard" && action == Some("save") {
            discard = true;
            continue;
        }
        let value = args
            .next()
            .ok_or_else(|| format!("{flag} requires a value"))?;
        match flag.as_str() {
            "--generation" if action.is_some() => generation = Some(value.clone()),
            "--dir" => dir = Some(PathBuf::from(value)),
            "--socket" => socket = Some(PathBuf::from(value)),
            "--file" => file = Some(value),
            "--cache" if register => target.cache = value.clone(),
            "--service" if register => target.service = value.clone(),
            "--resource" if register => target.resource = value.clone(),
            "--type" if register => target.resource_type = value.clone(),
            "--split" if register => target.split = value.clone(),
            "--branch" if !register => target.branch = Some(value.parse::<u16>()?),
            _ => return Err(format!("Unknown option {flag}").into()),
        }
    }
    if register
        && [
            &target.cache,
            &target.service,
            &target.resource,
            &target.resource_type,
        ]
        .iter()
        .any(|s| s.is_empty())
    {
        return Err("--cache, --service, --resource and --type are required".into());
    }
    if !register && target.branch.is_none_or(|id| id == 0) {
        return Err("--branch requires a registered nonzero branch ID".into());
    }
    let mut bytes = Vec::new();
    if action != Some("promote") {
        let file = file.ok_or("--file is required (use '-' for stdin)")?;
        let maximum = 16 << 20;
        if file == "-" {
            input.by_ref().take(maximum + 1).read_to_end(&mut bytes)?;
        } else {
            File::open(file)?
                .take(maximum + 1)
                .read_to_end(&mut bytes)?;
        }
        if bytes.is_empty() || bytes.len() as u64 > maximum {
            return Err("Expected a nonempty PIL payload of at most 16 MiB".into());
        }
    }
    let request = if register {
        json!({"op":"register", "target":{
            "cache": target.cache, "service":target.service, "resource":target.resource,
            "resource_type":target.resource_type, "split":target.split,"action":action,"generation":generation,"discard":discard
        }, "bytes":bytes})
    } else {
        json!({"op":"ingest", "branch":target.branch, "bytes":bytes})
    };
    let result = Connection::open(dir, socket)?.call(request)?;
    serde_json::to_writer(&mut *output, &result)?;
    writeln!(output)?;
    output.flush()?;
    Ok(())
}
