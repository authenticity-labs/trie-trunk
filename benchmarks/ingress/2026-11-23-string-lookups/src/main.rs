use std::time::Instant;
use string_lookups::{LOOKUPS, prepare, run};
fn main() {
    println!("case,round,algorithm,side,ns_per_lookup");
    for round in 0..19 {
        for index in 0..3 {
            let case = (round + index) % 3;
            prepare(case, round);
            for j in 0..10 {
                let test = (round * 3 + j) % 10;
                let (algorithm, side) = (test % 5, test / 5);
                let start = Instant::now();
                run(algorithm, side);
                let ns = start.elapsed().as_nanos() as f64 / LOOKUPS as f64;
                if round >= 2 {
                    println!("{case},{round},{algorithm},{side},{ns:.3}");
                }
            }
        }
    }
}
