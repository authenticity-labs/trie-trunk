//! File/stdin adapters over shared Ports. No node installation or registration.
use crate::Result;
use data_seeds::ports::{Limits, Ports, format::Profile, import::Mapping};
use std::io::{BufRead, BufReader, Read, Write};

fn profile(path: &str) -> Result<Profile> {
    let mut bytes = Vec::new();
    std::fs::File::open(path)?
        .take(32769)
        .read_to_end(&mut bytes)?;
    if bytes.len() > 32768 {
        return Err("Format profile exceeds 32 KiB".into());
    }
    Ok(serde_json::from_slice(&bytes)?)
}
pub(super) fn run(args: &[String], input: impl BufRead, output: &mut impl Write) -> Result<()> {
    match args{
        [op,path] if op=="inspect"=>inspect(path,input,output,None),
        [op,path,flag,file] if op=="inspect"&&flag=="--profile"=>inspect(path,input,output,Some(profile(file)?)),
        [op,path] if op=="review"=>review(path,input,output,false),
        [op,path,flag] if op=="review"&&flag=="--rx"=>review(path,input,output,true),
        _=>Err("Expected ports inspect <file|-> [--profile file.json] or ports review <mapping.json|-> [--rx]".into()),
    }
}
fn inspect(
    path: &str,
    input: impl BufRead,
    output: &mut impl Write,
    profile: Option<Profile>,
) -> Result<()> {
    let sample = if path == "-" {
        Ports.sample_with(input, Limits::default(), None, profile)?
    } else {
        let file = std::fs::File::open(path)?;
        let length = file.metadata()?.len();
        Ports.sample_with(
            BufReader::new(file),
            Limits::default(),
            Some(length),
            profile,
        )?
    };
    serde_json::to_writer(&mut *output, &sample)?;
    writeln!(output)?;
    Ok(())
}
fn review(path: &str, input: impl BufRead, output: &mut impl Write, rx: bool) -> Result<()> {
    let mut bytes = Vec::new();
    if path == "-" {
        input.take((1 << 20) + 1).read_to_end(&mut bytes)?;
    } else {
        std::fs::File::open(path)?
            .take((1 << 20) + 1)
            .read_to_end(&mut bytes)?;
    }
    if bytes.len() > 1 << 20 {
        return Err("Import mapping exceeds 1 MiB".into());
    }
    let mapping: Mapping = serde_json::from_slice(&bytes)?;
    if rx {
        output.write_all(mapping.prepare()?.document.bytes()?)?;
    } else {
        serde_json::to_writer(&mut *output, &Ports.review(&mapping)?)?;
        writeln!(output)?;
    }
    Ok(())
}
