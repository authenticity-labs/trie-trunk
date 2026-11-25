//! Go selects an RX and passes host values. The Seed prescription owns the PIL.
use super::{SeedSlice, input, invalid, output};
use ciborium::Value;
use data_seeds::{
    SeedDefinition,
    seed::log::{Logger, Style},
};
use std::ffi::c_void;

#[unsafe(no_mangle)]
pub unsafe extern "C" fn trunk_log_definition(name: SeedSlice) -> *mut c_void {
    output((|| match unsafe { input(name.data, name.len)? } {
        b"log.http" => Ok(data_seeds::seed::log::HTTP.to_vec()),
        b"log.query" => Ok(data_seeds::registration::query_events::definition()?
            .bytes()?
            .to_vec()),
        _ => Err(invalid(
            "Choose log.http or log.query, or use OpenLog with a registered RX",
        )),
    })())
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn trunk_log_open(
    rx: SeedSlice,
    style: u8,
    handle: *mut *mut c_void,
) -> *mut c_void {
    output((|| {
        if handle.is_null() {
            return Err(invalid("Missing log handle"));
        }
        let def = SeedDefinition::read(unsafe { input(rx.data, rx.len)? })?;
        let style = match style {
            0 => Style::Trunk,
            1 => Style::OpenTelemetry,
            _ => return Err(invalid("Unknown logger style")),
        };
        let logger = def.logger(style)?;
        let digest: String = def.digest().iter().map(|b| format!("{b:02x}")).collect();
        let options = serde_json::to_vec(
            &serde_json::json!({"options":logger.options(), "custom":logger.custom_fields(), "digest":digest}),
        )
        .map_err(|e| invalid(e.to_string()))?;
        unsafe {
            *handle = Box::into_raw(Box::new(logger)).cast();
        }
        Ok(options)
    })())
}

/// Match an unversioned producer template to the revision stamped at registration.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn trunk_log_release_digest(rx: SeedSlice, major: u32, minor: u32) -> *mut c_void {
    output((|| {
        let doc = data_seeds::doc::read(unsafe { input(rx.data, rx.len)? })?;
        if doc.definitions()[0].header.rx_major != 0 {
            return Err(invalid("Published logger Definition does not match branch"));
        }
        let doc = data_seeds::registration::stamp(doc, major, minor)?;
        Ok(doc.definitions()[0].header.rx_hash.clone())
    })())
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn trunk_log_encode(
    handle: *const c_void,
    time: u64,
    entropy: SeedSlice,
    values: SeedSlice,
) -> *mut c_void {
    output((|| {
        let logger = unsafe { handle.cast::<Logger>().as_ref() }
            .ok_or_else(|| invalid("Logger is closed"))?;
        let id = unsafe { input(entropy.data, entropy.len)? }
            .try_into()
            .map_err(|_| invalid("Invalid log entropy"))?;
        let mut event = logger.event(time, id)?;
        type Fields = Vec<(String, Value)>;
        let (options, custom): (Fields, Fields) =
            ciborium::from_reader(unsafe { input(values.data, values.len)? })
                .map_err(|e| invalid(e.to_string()))?;
        for (name, value) in options {
            event.set(&name, value)?;
        }
        for (name, value) in custom {
            event.field(&name, value)?;
        }
        event.encode()
    })())
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn trunk_log_close(handle: *mut c_void) {
    if !handle.is_null() {
        drop(unsafe { Box::from_raw(handle.cast::<Logger>()) });
    }
}
