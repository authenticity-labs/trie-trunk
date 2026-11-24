# Implementation Map

This is a version-zero project. Storage-format experiments replace disposable
fixtures; they do not require migrations or compatibility readers. Historical
benchmark reports describe the source and format measured at that time.

The proposed next boundary is documented in the [Native Record Engine Plan](NATIVE-ENGINE-PLAN.md).
The table below describes the current implementation, not that proposed endpoint.

## Ownership and Boundaries

| Layer | Owner | Main entry points |
| --- | --- | --- |
| Producer, definition negotiation, PIL ingestion | Go | `go/logger/client`, `go/logger/pil`, `go/server/cache/http.go`, `generic_http.go` |
| Configuration, HTTP, authentication, source routing | Go | `go/cmd/trunk`, `go/server/serve`, cache HTTP handlers |
| Live event/object/revision directories | Rust | `rust/src/live.rs`, `go/server/cache/native_live.go` |
| Sealing, canonical files, replay assembly, retention | Go | `go/server/cache` |
| Tantivy indexes, readers/writers, postings, candidate collection | Rust | `rust/src/tantivy` |
| Native request/result ownership | Go/C ABI/Rust | `search_native.h`, `search_native*.go`, `tantivy/native` |
| Browser storage and queries | SharedWorker + Rust/WASM | `../snoot/client/services/store`, `rust/browser`, `pil/wasm` |
| PIL/PAC definition and encoding | Rust and Go implementations | `pil`, `go/logger/pil`; cross-language fixtures test agreement |

Snoot calls Tantivy in-process. There is no JSON or CBOR request pipe on that path.
Go projects PIL values, constructs a bounded native request arena, and calls the
C ABI. Rust owns its search engine state and returned native buffers. Go copies
results before freeing those buffers; Rust must not retain Go pointers. Native
cancellation and source pinning remain part of this ownership contract.

Resident PIL bytes and recovery/delivery buffers now use Rust-owned allocations
with explicit snapshot pins (`native_memory.go`, `rust/src/buffers.rs`).
`native_ingress.go` shares append-only native chunks across ingress records and
request boundaries; only unused suffixes are writable. Go
holds borrowed slice descriptors, not duplicate resident payload allocations.

This does **not** make the entire datastore GC-free. Live identity directories
are Rust-owned. Projection slices/strings, query/live-view snapshot metadata,
temporary sealing descriptors and copied candidate results still live in Go.
Sealing, cold payload assembly and codec execution still allocate Go buffers. Memory-mapped index bytes and Rust index structures do not live
on the Go heap. Allocation measurements must include native memory and resident
set size alongside Go `B/op` and `allocs/op`.

The older standalone `trunk-search` CLI and `go/server/store` remain separate
implementations. Their CBOR file readers are not the canonical Snoot cache path.
Migration commands and browser imports of obsolete prototype caches are removed.

## Cache Modules

| Concern | Files in `go/server/cache` |
| --- | --- |
| Lifecycle and recovery | `store.go`, `store_open.go` |
| Ingress staging and duplicate checks | `writer.go` |
| Native live lookup and expiry | `native_live.go`, `rust/src/live.rs` |
| Frozen-hour preparation and publication | `seal.go`, `seal_scheduler.go` |
| Object identity and revision access | `objects.go`, `collection_*.go` |
| Invalidation and replay state tokens | `object_state.go` |
| Immutable query snapshots | `view_snapshot.go` |
| Cursor signing and merge order | `view_cursor.go`, `view_merge.go` |
| HTTP response assembly | `view_http.go` |
| PIL-to-search projection | `search_projection.go` |
| Tantivy catch-up and live reconciliation | `search_ingest.go` |
| Predicate binding and candidate intersection | `search_query.go` |
| Background scheduling and source gates | `search_worker.go` |
| Native arena and ABI lifecycle | `search_native_arena.go`, `search_native.go` |

## Rust Search Modules

`tantivy/mod.rs` defines internal types. `handle.rs` owns schema construction and
index writes; `query.rs` owns predicates and queries; `lifecycle.rs` dispatches
operations. `native/mod.rs` handles ABI conversion and result ownership,
`native/registry.rs` pins engine handles and coordinates concurrent calls, and
`native/collector.rs` handles cancellable collection.

The cleanup changes module boundaries, not storage layouts, addresses, query
semantics or publication rules. Native projection and collection improvements
require separate measurements and correctness tests; see
[remaining optimizations](OPTIMIZATIONS.md).

## Validation

Build the Rust static library before Go (`make rust`). Then run the Go suite,
Rust library suite, frontend type checks and frontend tests. Browser store tests
exercise OPFS publication and restart separately in Chromium and WebKit.
Production full-text search requires its index; a direct scan exists only as an
explicit benchmark control, not a silent serving fallback.

## Sealing Concurrency

`Store.mu` protects the publication boundary. Preparation pins one native hour,
uses a private trie, then releases the lock before waiting for a worker permit,
packing and writing canonical tails. Pending identities remain searchable until
publication. Collection updates, seed cleanup and time-trie publication still
hold the lock; timings are exposed rather than calling the whole path nonblocking.
Close/retention wait outside the lock. The commit journal remains authoritative
for crash recovery; the seed watermark certifies interval completeness.
