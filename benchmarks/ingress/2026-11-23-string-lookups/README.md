# Common-String Dictionary Lookup

Warm, in-memory UTF-8 string to BOX ordinal lookup, measured on one arm64 Mac running macOS 27.2.1 with rustc 1.97.1. Native and browser runs were serial. Production code and stored dictionaries were read-only.

## Data

- **Stored telemetry** — A read-only snapshot of the local `demo/trunk-client/queries/query.v0.packs` yielded 639 events through the registered Seed reader. We counted the six text fields (operation, status, source, query, error, user_agent) and kept values longer than two bytes. The [corpus](telemetry.json) contains all 29 distinct eligible literals and their counts. For comparison we put the 19 repeated values in a resident table (3,180 occurrences, 5–135 bytes) and used the 10 singletons as observed misses (9–117 bytes). Probe selection is weighted by occurrence within each group. This is a benchmark admission rule. The actual local query and lab BOXes had zero promoted entries.
- **Replication fixture** — The existing sealed /strings fixture has a single BOX entry, `events | where duration > 10000`. We looked it up alongside other observed query strings beginning with `events | where`, preserving their frequencies in the miss stream.
- **Sealing fixture** — All 512 distinct messages from the [existing sealing benchmark](../../../rust/seeds/src/packs/origin/sealing.rs), generated with the same prefix and channel IDs. Misses use 512 disjoint channel IDs with the same message length and prefix. This workload is synthetic.

## Method

Each candidate hashes the complete UTF-8 probe, searches the same hashbrown table, checks exact string equality on potential matches and returns the ordinal or absence. SHA candidates compute their ordinary digest and use its first eight bytes as the table hash. aHash uses a key chosen per round. The current-path control uses the production directory's standard-library `HashMap<Arc<str>, usize>::get(&str)` with its default hasher. The four candidates share a table implementation with each other. Comparisons against the current control include any table implementation differences. Dictionary keys and probes have separate allocations.

A round creates a new table and randomizes 4,096 probes per side with a reproducible seed shared across all algorithms and targets. Every probe is checked against the control before timing. Timed Rust loops accumulate and verify the returned ordinals. Each sample includes 131,072 lookups or a multiple thereof. Two warmup rounds precede 17 measured rounds per cell. Execution order rotates. Reported times are medians of **batch averages**, in nanoseconds per lookup. WebKit groups 16 batches for the fast algorithms and two for SHA to reduce clock quantization. There is no JS call per key. Results measure a single directory probe. They exclude admission, insertion, staged additions, reference encoding, PAC sealing, storage I/O and the separate SHA-256 dictionary digest.

Rust's native release settings were used for macOS. The browser profile follows trunk-browser's size optimization and LTO settings. Chrome 157 and Playwright WebKit 26.5 ran the same wasm32-unknown-unknown code in a dedicated worker. The regular SHA rows use RustCrypto's sha1 0.10.7 and sha2 0.10.9 with default features. These builds use software SHA on ARM64. A separate control calls macOS CommonCrypto's CC_SHA1 and CC_SHA256 once per lookup and includes their API overhead.

## Measurements

### Stored Telemetry

Median ns per lookup, shown as hit / miss.

| Hasher | macOS Rust | Chrome WASM | WebKit WASM |
|---|---:|---:|---:|
| XXH3-64 | 11.2 / 7.8 | 33.6 / 19.1 | 31.5 / 16.7 |
| SHA-1 | 96.8 / 92.6 | 264.7 / 229.6 | 305.2 / 278.5 |
| SHA-256 | 237.5 / 228.0 | 457.0 / 420.4 | 442.5 / 415.8 |
| aHash | 10.8 / 6.9 | 42.0 / 27.5 | 38.1 / 25.7 |
| Current Rust lookup | 22.1 / 20.0 | 39.7 / 23.7 | 39.1 / 25.7 |

Apple CommonCrypto control on macOS: SHA-1 185.9 / 156.6 ns; SHA-256 183.1 / 154.6 ns.

### Single-Entry Replication Fixture

Median ns per lookup, shown as hit / miss.

| Hasher | macOS Rust | Chrome WASM | WebKit WASM |
|---|---:|---:|---:|
| XXH3-64 | 6.2 / 4.7 | 28.2 / 10.7 | 25.3 / 10.0 |
| SHA-1 | 85.5 / 71.9 | 217.4 / 190.7 | 244.1 / 232.7 |
| SHA-256 | 186.0 / 185.3 | 376.9 / 346.4 | 354.8 / 343.3 |
| aHash | 6.0 / 4.1 | 35.9 / 19.1 | 32.9 / 18.6 |
| Current Rust lookup | 19.0 / 13.7 | 39.7 / 16.8 | 36.2 / 19.1 |

Apple CommonCrypto control on macOS: SHA-1 153.7 / 143.5 ns; SHA-256 153.3 / 140.2 ns.

### 512-Entry Sealing Fixture

Median ns per lookup, shown as hit / miss.

| Hasher | macOS Rust | Chrome WASM | WebKit WASM |
|---|---:|---:|---:|
| XXH3-64 | 9.5 / 7.1 | 48.1 / 17.5 | 46.7 / 16.2 |
| SHA-1 | 143.7 / 141.1 | 403.6 / 373.8 | 469.2 / 442.5 |
| SHA-256 | 374.0 / 374.9 | 707.2 / 677.5 | 682.8 / 656.1 |
| aHash | 9.2 / 6.5 | 60.3 / 30.5 | 56.3 / 29.1 |
| Current Rust lookup | 23.6 / 20.0 | 58.0 / 24.4 | 52.5 / 25.3 |

Apple CommonCrypto control on macOS: SHA-1 225.7 / 220.2 ns; SHA-256 219.2 / 213.1 ns.

## Reproduction

Run from this directory. Set PLAYWRIGHT_MODULE to the installed Playwright Core module when it is outside the Node module search path.

```sh
cargo run --release --locked > macos.csv
cargo run --release --locked --features apple-sha > macos-apple.csv
cargo build --target wasm32-unknown-unknown --profile browser --lib --locked
node browser.mjs target/wasm32-unknown-unknown/browser/string_lookups.wasm chromium > chrome.csv
node browser.mjs target/wasm32-unknown-unknown/browser/string_lookups.wasm webkit > webkit.csv
```

CSV case IDs 0, 1 and 2 denote replication, telemetry and sealing. Algorithm IDs 0 through 4 denote XXH3-64, SHA-1, SHA-256, aHash and the current Rust lookup. Side 0 is a hit and side 1 is a miss. [macOS samples](macos.csv) · [macOS CommonCrypto control](macos-apple.csv) · [Chrome samples](chrome.csv) · [WebKit samples](webkit.csv)

## Sealing With the Integrated Dictionary

The existing release-mode hourly sealing benchmark ran once with each implementation on the same Mac. This includes the BOX lookup, qualification, PAC encoding, CRC, compression and in-memory commit. It excludes disk I/O. Qualification here is the benchmark's explicit on/off setting. In normal ingestion the monthly admission policy still determines eligible strings.

| Warm BOX, 100,000 events | Previous median ms | XXH3-64 median ms |
|---|---:|---:|
| Rare strings | 26.335 | 24.443 |
| 8 repeated strings | 34.295 | 33.095 |
| 512 repeated strings | 36.915 | 35.651 |

The stored byte counts, uncompressed lengths and directory entry counts matched in every corresponding sample. There were 624 samples per build over tiny and full-sized PACs, warm and new dictionaries, and rare and repeated strings. Each build ran its existing internal on/off pairs. [Raw sealing samples](sealing.csv)
