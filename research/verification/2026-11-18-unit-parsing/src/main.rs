use std::{hint::black_box, time::Instant, sync::atomic::{AtomicUsize, Ordering}};
struct Budget(AtomicUsize);
impl fend_core::Interrupt for Budget {
    fn should_interrupt(&self) -> bool { self.0.fetch_add(1, Ordering::Relaxed) >= 100_000 }
}
fn fend(s: &str) -> Result<String, String> {
    fend_core::evaluate_with_interrupt(s, &mut fend_core::Context::new(), &Budget(AtomicUsize::new(0)))
        .map(|q| q.get_main_result().to_owned())
}
fn main() {
    let cases = ["42", "9007199254740993", "340282366920938463463374607431768211455", "1e3 ms", "250ms", "250 ms", "250 milliseconds", "250 msec", "1.5 seconds", "1 sec", "10 µs", "10 μs", "10 us", "6.5ft", "6 feet", "5'10\"", "60 km/h", "60 kilometres per hour", "25 °C", "98.6 °F", "12 MiB", "12 MB", "12 mb", "12%", "$12.50", "¥1200", "£12.50", "€12.50", "12.50 USD", "$1,234.56", "€1.234,56", "1,234", "1,5 s", "1\u{202f}234,5 ms", "-12 ms", "−12 ms", "(12.50)", "duration=250ms", "elapsed 250 ms", "error 500 after 2 s", "unknown", "NaN", "inf", "10m", "1/2", "x=10", "1+2", "12 ms garbage"];
    println!("INPUT\tIRIDIUM\tRUNITS\tFEND");
    for s in cases {
        let a = iridium_units::parse_quantity(s).map(|q| format!("{:.17} {}", q.value(), q.unit())).map_err(|e|e.to_string());
        let b = runits::Quantity::parse(s).map(|q| format!("{:.17};base={:.17}",q.value(),q.base())).map_err(|e|e.to_string());
        println!("{s:?}\t{a:?}\t{b:?}\t{:?}", fend(s));
    }
    println!("CONVERSIONS");
    for (s,target) in [("250 ms","s"),("6 feet","m"),("25 °C","°F"),("12 MiB","bytes"),("9007199254740993 ms","ms")] {
        let a=iridium_units::parse_quantity(s).and_then(|q| iridium_units::parse_unit(target).and_then(|t|q.to_value(t)));
        let b=runits::Quantity::parse(s).map_err(|e|e.to_string()).and_then(|q|q.to(target).map(|q|q.value()).map_err(|e|e.to_string()));
        println!("{s} -> {target}\t{a:?}\t{b:?}\t{:?}",fend(&format!("{s} to {target}")));
    }
    println!("BENCH median ns/parse, 7 rounds x 2000 inputs, first round discarded");
    for s in ["250 ms","6 feet","60 km/h","$12.50"] {
        let mut timings=[Vec::new(),Vec::new(),Vec::new()];
        for round in 0..8 {
            for offset in 0..3 {
                let which=(round+offset)%3;
                let start=Instant::now();
                for _ in 0..2000 { match which {
                    0=>{let _=black_box(iridium_units::parse_quantity(black_box(s)));},
                    1=>{let _=black_box(runits::Quantity::parse(black_box(s)));},
                    _=>{let _=black_box(fend(black_box(s)));},
                }}
                if round>0 {timings[which].push(start.elapsed().as_nanos()/2000);}
            }
        }
        for samples in &mut timings {samples.sort();}
        println!("{s:?}\tiridium={}\trunits={}\tfend={}",timings[0][3],timings[1][3],timings[2][3]);
    }
}
