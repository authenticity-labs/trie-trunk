//! Command adapters use the same Node and query engine as socket/HTTP clients.
mod writes;
mod ports;
use crate::{Node, Result, Storage};
use std::{
    io::{BufRead, Write},
    path::Path,
};
pub fn run(
    args: &[String],
    input: impl BufRead,
    output: &mut impl Write,
    diagnostics: &mut impl Write,
    terminal: bool,
) -> Result<()> {
    if args.first().is_some_and(|s|s=="ports") {return ports::run(&args[1..],input,output);}
    if args.first().is_some_and(|s| s == "search") {
        let mut query = vec!["beds".into(), "query".into()];
        query.extend_from_slice(&args[1..]);
        return crate::cli::run_output(&query, input, output, diagnostics, terminal);
    }
    if args
        .first()
        .is_some_and(|s| matches!(s.as_str(), "definitions" | "seeds"))
    {
        return writes::run(args, input, output);
    }
    if args
        .first()
        .is_some_and(|s| matches!(s.as_str(), "beds" | "time"))
    {
        return crate::cli::run_output(args, input, output, diagnostics, terminal);
    }
    match args {
        [command,root,name,flag,storage] if command=="install" && flag=="--storage" => {
            let storage:Storage=serde_json::from_value(serde_json::Value::String(storage.clone()))?;
            let node=Node::install_with(Path::new(root),name,storage)?;
            writeln!(output,"{}",serde_json::to_string(node.configuration())?)?;
        }
        [command,root,name] if command=="install" => {
            let node=Node::install(Path::new(root),name)?;
            writeln!(output,"{}",serde_json::to_string(node.configuration())?)?;
        }
        [command,root] if command=="inspect" => {
            let node=Node::open(Path::new(root))?;
            writeln!(output,"{}",serde_json::to_string(node.configuration())?)?;
        }
        [command,length] if command=="candidate" => {
            let candidate=data_seeds::ports::Ports.sample(input,data_seeds::ports::Limits::default(),Some(length.parse()?))?;
            writeln!(output,"{}",serde_json::to_string(&candidate)?)?;
        }
        [command,length,profile] if command=="candidate" => {
            let profile=serde_json::from_str(profile)?;
            let candidate=data_seeds::ports::Ports.sample_with(input,data_seeds::ports::Limits::default(),Some(length.parse()?),Some(profile))?;
            writeln!(output,"{}",serde_json::to_string(&candidate)?)?;
        }
        _ => return Err("Expected install <directory> <name> [--storage pack|browse], inspect <directory>, candidate <source-byte-length>, beds query, or time query".into()),
    }
    output.flush()?;
    Ok(())
}
