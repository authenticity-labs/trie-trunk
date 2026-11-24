use serde_json::{Value, json};
use std::cell::RefCell;
thread_local! {static INPUT:RefCell<Vec<u8>>=RefCell::new(Vec::new());static OUTPUT:RefCell<Vec<u8>>=RefCell::new(Vec::new());}
#[unsafe(no_mangle)]
pub extern "C" fn input(n: usize) -> *mut u8 {
    INPUT.with_borrow_mut(|b| {
        b.resize(n, 0);
        b.as_mut_ptr()
    })
}
#[unsafe(no_mangle)]
pub extern "C" fn execute() {
    let result = INPUT.with_borrow(|b| -> Result<Value, String> {
        let v: Value = serde_json::from_slice(b).map_err(|e| e.to_string())?;
        data_seeds::language::request(&v)
    });
    OUTPUT.with_borrow_mut(|b| {
        b.clear();
        serde_json::to_writer(
            b,
            &match result {
                Ok(value) => json!({"value":value}),
                Err(error) => json!({"error":error}),
            },
        )
        .unwrap()
    });
}
#[unsafe(no_mangle)]
pub extern "C" fn output() -> *const u8 {
    OUTPUT.with_borrow(|b| b.as_ptr())
}
#[unsafe(no_mangle)]
pub extern "C" fn output_len() -> usize {
    OUTPUT.with_borrow(Vec::len)
}
