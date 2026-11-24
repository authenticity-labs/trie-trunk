//! Reproduce the summary class library through the DOC builder.
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let destination = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "definitions/summary.rx".into());
    data_seeds::summaries::summary_template()?.write(destination)?;
    Ok(())
}
