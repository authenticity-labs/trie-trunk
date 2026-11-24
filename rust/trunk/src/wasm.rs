use super::*;
use serde_json::Value;
use std::cell::RefCell;
#[derive(Default)]
struct Runtime {
    input: Vec<u8>,
    output: Vec<u8>,
    error: String,
    trees: Vec<Option<Trunk>>,
}
thread_local! {static R:RefCell<Runtime>=RefCell::new(Runtime::default());}
#[unsafe(no_mangle)]
pub extern "C" fn trunk_input(n: usize) -> *mut u8 {
    R.with_borrow_mut(|r| {
        r.input.resize(n, 0);
        r.input.as_mut_ptr()
    })
}
fn run(f: impl FnOnce(&mut Runtime) -> Result<u32, Error>) -> u32 {
    R.with_borrow_mut(|r| match f(r) {
        Ok(n) => n,
        Err(e) => {
            r.error = e;
            u32::MAX
        }
    })
}
#[unsafe(no_mangle)]
pub extern "C" fn trunk_open() -> u32 {
    run(|r| {
        let tree = Trunk::open(std::mem::take(&mut r.input))?;
        if let Some(id) = r.trees.iter().position(Option::is_none) {
            r.trees[id] = Some(tree);
            return Ok(id as u32);
        }
        let id = u32::try_from(r.trees.len()).map_err(|_| "Too many trunk handles")?;
        if id == u32::MAX {
            return Err("Too many trunk handles".into());
        }
        r.trees.push(Some(tree));
        Ok(id)
    })
}
#[unsafe(no_mangle)]
pub extern "C" fn trunk_release(id: usize) {
    R.with_borrow_mut(|r| {
        if let Some(t) = r.trees.get_mut(id) {
            *t = None;
        }
    })
}
#[unsafe(no_mangle)]
pub extern "C" fn trunk_query(id: usize) -> u32 {
    run(|r| {
        let t = r
            .trees
            .get(id)
            .and_then(Option::as_ref)
            .ok_or("Released trunk trie")?;
        let v: Value = serde_json::from_slice(&r.input).map_err(|e| e.to_string())?;
        let out = t.query(&v)?;
        r.output.clear();
        serde_json::to_writer(&mut r.output, &out).map_err(|e| e.to_string())?;
        Ok(0)
    })
}
#[unsafe(no_mangle)]
pub extern "C" fn trunk_section(id: usize, section: u32) -> u32 {
    run(|r| {
        let t = r
            .trees
            .get(id)
            .and_then(Option::as_ref)
            .ok_or("Released trunk trie")?;
        let bytes = t.section(section).ok_or("Unknown registry section")?;
        r.output.clear();
        r.output.extend_from_slice(bytes);
        Ok(0)
    })
}
#[unsafe(no_mangle)]
pub extern "C" fn trunk_output() -> *const u8 {
    R.with_borrow(|r| r.output.as_ptr())
}
#[unsafe(no_mangle)]
pub extern "C" fn trunk_output_len() -> usize {
    R.with_borrow(|r| r.output.len())
}
#[unsafe(no_mangle)]
pub extern "C" fn trunk_error() -> *const u8 {
    R.with_borrow(|r| r.error.as_ptr())
}
#[unsafe(no_mangle)]
pub extern "C" fn trunk_error_len() -> usize {
    R.with_borrow(|r| r.error.len())
}

/// File integrity is checked at replication, never on each field access.
#[cfg(feature = "registration")]
#[unsafe(no_mangle)]
pub extern "C" fn trunk_check_definition_file(id: usize, name_length: usize) -> u32 {
    run(|r| {
        let t = r.trees.get(id).and_then(Option::as_ref).ok_or("Released trunk trie")?;
        let name = std::str::from_utf8(r.input.get(..name_length).ok_or("Invalid filename length")?)
            .map_err(|e| e.to_string())?;
        let bytes = &r.input[name_length..];
        if !t.definition_file(name)?.matches(bytes) {
            return Err("Definition file does not match registered revision".into());
        }
        Ok(0)
    })
}
