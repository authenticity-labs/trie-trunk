use std::{
    io::{self, BufRead, Write, IsTerminal},
    path::Path,
};
use trunk_node::{Node, Result};

fn run() -> Result<()> {
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    if args.first().is_none_or(|command| command != "serve") {
        return trunk_node::command::run(&args, io::stdin().lock(), &mut io::stdout().lock(), &mut io::stderr().lock(), io::stdout().is_terminal());
    }
    match args.as_slice() {
        [command, root, socket] if command == "serve" => {
            let node = std::sync::Arc::new(Node::open(Path::new(root))?);
            let _service = trunk_node::service::Service::start(node.clone(), Path::new(socket))?;
            let mut input = io::stdin().lock();
            let mut output = io::stdout().lock();
            // Control messages only. No event/data payload copies through this
            // lifecycle channel; application query transport is separate.
            let mut line = String::new();
            loop {
                line.clear();
                let n = std::io::Read::take(&mut input, 4097).read_line(&mut line)?;
                if n == 0 { break; }
                if n > 4096 { return Err("Control request exceeds 4096 bytes".into()); }
                let response = match line.trim() {
                    "status" => serde_json::json!({"ok":true,"configuration":node.configuration()}),
                    "close" => break,
                    _ => serde_json::json!({"ok":false,"error":"Unknown lifecycle action"}),
                };
                writeln!(output,"{response}")?;output.flush()?;
            }
        }
        _ => return Err("Expected install <directory> <name> [--storage pack|browse], inspect <directory>, candidate <source-byte-length>, or serve <directory> <socket>".into()),
    }
    Ok(())
}
fn main() {
    if let Err(error) = run() {
        eprintln!("{error}");
        std::process::exit(1);
    }
}
