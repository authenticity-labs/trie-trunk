# Leaf Query Planning

The native release reader was measured on the same Mac before and after grouping consecutive leaf decisions and retaining the index plan across cursor polls. Both builds already pruned leaf predicates. The fixture contains two hours of 10,000 events, two Definition versions, repeated leaf IDs and a projection of `sequence`. Pack and index files use the reader's in-memory positional storage fixture. Timings exclude dataset construction, disk latency, transport and UI rendering.

| Query | Results | Before µs | After µs | PAC bytes after | Index bytes after |
|---|---:|---:|---:|---:|---:|
| East, fetch selected rows | 10,000 | 315.3 | 200.2 | 60,100 | 120,024 |
| East, fetch and materialize objects | 10,000 | 1,550.8 | 1,468.3 | 160,100 | 120,024 |
| North, fetch and materialize objects | 0 | 185.5 | 21.6 | 0 | 120,012 |
| Unfiltered, fetch and materialize objects | 20,000 | 2,901.1 | 2,864.3 | 260,100 | 12 |

Seven pairs of processes alternated which build ran first. Each process reported the median of nine executions per case. This table reports the median of those seven medians, with pruning enabled on both sides. Every run checked result counts and compared projected objects with the reference row scanner. [Per-run results, including controls with pruning disabled](reader.csv)

The benchmark can be rerun with `cargo test --offline --release --manifest-path rust/seeds/Cargo.toml --test reader_index leaf_pruning_baseline -- --ignored --nocapture`. Separate regression tests exercise reused leaf IDs across version boundaries, bounded cooperative work, partial and disjoint windows, residual predicates, global limits and offsets.

## Timestamp Predicate Pushdown

The same fixture compares a reference row scan with a seek on the registered
timestamp. The window selects milliseconds 500 through 599 of the second hour.
Both paths return the projected `sequence` values. These are medians of nine
paired executions in one release process on macOS, alternating execution order.
Byte counts measure reads requested from the in-memory positional storage.

| Predicate | Rows returned | Ordinals visited, scan → seek | PAC bytes, scan → seek | Index bytes, scan → seek | Time, scan → seek |
|---|---:|---:|---:|---:|---:|
| Timestamp window | 100 | 20,000 → 100 | 261,112 → 31,050 | 120,012 → 36,840 | 1,236.9 → 22.5 µs |
| Timestamp window and `sequence >= 575` | 25 | 20,000 → 100 | 260,362 → 95,836 | 120,012 → 53,220 | 1,271.5 → 24.1 µs |
