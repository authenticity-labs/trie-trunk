//! Export the same application annotation catalogue used by Snoot into DOC.
fn main() -> Result<(), Box<dyn std::error::Error>> {
    serde_json::to_writer_pretty(
        std::io::stdout().lock(),
        &data_seeds::ports::editor::catalog(),
    )?;
    Ok(())
}
