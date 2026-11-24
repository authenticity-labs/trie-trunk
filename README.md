# Trie-Trunk

An open-source time-series database for typed events, temporal search, and
precomputed analytics. Optional collection indexing supports object revision replay.
Ingest typed events once; reuse complete hourly datasets and compact summaries in
the browser instead of repeating every investigation against the server.

[Website](https://trietrunk.com) · [Snoot](https://snoot.trietrunk.com) ·
[PIL Bootstrap Definition (.rx)](https://github.com/authenticity-labs/pill-packer/blob/main/schemas/pil.rx)

Version zero: storage designs and fixtures can change without migration paths.
The sample datasets are disposable.

## Database And Client Boundary

Trunk is a self-sufficient database. The Snoot browser app, desktop shell, and static web server live in the sibling `snoot` project. Snoot is an independent client, alongside
the CLI, API consumers, and future SQL IDE integrations. No database operation
may require Snoot, its lifecycle, or its presentation state.

All nodes use the same Rust database source. Configuration selects packed
`.packs` or browsable `.beds` storage; it does not select a different ingestion,
recovery, summary, publication, or query implementation. Platform adapters supply
filesystem, transport, and execution primitives. Bundling Trunk with Snoot.app
does not create a desktop database variant.

The registered HTTP path and Snoot's socket use the same `rust/node::Node` owner.
Snoot bundles the public `trunk` executable and starts `trunk serve` with a private
control socket. Go supplies transport adapters; it no longer has a separate
registered-event recovery, file-rotation, summary, or commit coordinator.
Definition plans are shared with HTTP readers. Live snapshots and published
file handles are captured under the node's commit lock, then streamed through
the existing pooled buffers and coalesced ranges. Transport buffers use the
node's memory admission budget too.

Request observations use the existing `log.http` Definition and the same Rust
ingestion method. Go captures transport timings and response sizes; it does not
encode those events or negotiate an older logger Definition. Ordinary query
telemetry continues to use `log.query`.

Admission makes the recovery record durable before exposing the event.
Publication retains encoded buffers and flushes the Pack and index before
publishing Time. Browser workers still use the shared Seed/WASM libraries and
their browser I/O adapter. The older object-index prototype remains separate;
this cutover does not certify or replace that prototype.

The HTTP command accepts `--storage pack|browse`; this selects the same Rust
storage policy used by the node library. Changing that setting does not convert
existing files. Opening a datastore that contains only the other representation
fails before recovery or publication, rather than creating an empty alternative.

## Shared CLI Commands

```sh
trunk definitions register --socket /path/node.sock --cache local \
  --service lab --resource readings --type sample --file sample.rx
trunk seeds load --socket /path/node.sock --branch 1 --file event.pil
trunk beds query --socket /path/node.sock --scope local/lab/readings/sample \
  --from 2026-11-18T00:00:00Z --to 2026-11-19T00:00:00Z \
  --sql 'SELECT * FROM events' --json
```

Use `--dir` instead of `--socket` only when no other process owns that directory.
`--file -` reads raw PIL bytes from stdin. `search` is an alias for `beds query`,
using the same Rust options and evaluator. The standalone app bundles this same
command implementation. Ingestion acknowledgements follow durable recovery
storage; queries include current in-memory events.

The retained object-index experiments require an explicit `trunk_prototype`
build tag and `trunk prototype cache|tap|search ...`; they are not a fallback in
normal nodes. Their source is preserved for further object-storage work.

## Seed Library

`rust/seeds` is the Rust `data-seeds` library. Seed Definitions register through
Pill Packer's DOC API. Seed setters and Seed Pack loading delegate to its existing
Rust builders, including capped byte appends and uncapped builder batches.
Application annotations remain attached to their field ordinals.

Seed Packs execute filters. PACKS/BEDS coordinate selected Packs, projection,
ordering, and one global offset/limit. An unordered cursor can stop as soon as
that limit is reached. The initial ordered implementation uses a bounded top-K
heap and requires a limit; it scans all selected candidates because no sorted-Pack
guarantee is assumed. The query coordinator does not perform external sorting
or parallel scans. Storage readers, writers, and publication have their own
Seed APIs; browser workers and platform I/O bindings supply the environment adapters.
The optional Rust SQL front end lowers statements into the shared query model.
Registered Snoot queries and server resource reads use these Rust bindings.
Remaining older callers are listed under Cleanup Review below.

```javascript
const seeds = await createSeeds();
const definition = seeds.prescribe(definitionBytes);
const packs = seeds.packs().pack(definition.open(packBytes));
const query = packs.query()
  .filter('reading', '>=', 100)
  .select('time', 'reading')
  .orderBy('time', 'desc')
  .limit(10);
const batch = query.fetch(10);
const rows = batch.views(); // Read fields on demand.
// batch.objects() instead returns independent, mutable objects.
```

Release queries and batches with `.dispose()` when finished. Views retain their
batch and backing bytes; materialized objects are independent of the batch.
Missing fields are omitted, null stays null, and integers outside JavaScript's
safe numeric range become `bigint`. Non-text-keyed maps become `Map` objects.
`seeds.packs(boxBytes, registeredDefinitions)` binds Box children without copying
plain Pack bytes. Definitions are supplied in child ordinal order by the caller.
BEDS exposes the same query interface.

Registered event data is accessed through
`trunk.trie().branches().select(branchIds).packs()` or `.beds()`, using the
same query coordinator on any node. The scope resolves each
Branch ID to its exact prepared Definition in `trunk.trie`; load its separate
`.rx` resource with `trunk.prescribe(definitionId, rxBytes)` first. Omitting
`select()` includes all registered branches; an empty selection includes none.
Use `packs.load(branchId, packBytes, maximumBytes)` for stored Packs, or
`packs.pack(branchId, pack)` to retain an already opened, index-bound Pack.
Both feed the same Rust query coordinator, with ordering and limits applied
across the selected Packs. Native shared-buffer ranges retain the existing
allocation; scopes and cursors remain usable after the registry handle closes.
`packs.loadBox(branchId, boxBytes, addresses)` opens selected children through
the existing Box reader, using start addresses supplied by the time trie.

PACKS/BEDS storage Boxes use `create_box()` with their directory disabled.
The existing time-trie lookup supplies Pack start addresses; the Rust PAC reader
reads the leading control byte and sized length to locate each Pack's end.
`open_box_addresses()` selects those ranges natively, and
`seeds.beds(boxBytes, definitions, addresses)` exposes the same operation to a
browser. This adds no hour-to-ordinal mapping and does not change time-trie bytes.
Summary Boxes retain their Definition/version information; equal-sized daily
Packs can use stride addressing, while size changes require a directory.
The standalone `seeds.summaries(boxBytes, firstPack, endPack)` reader selects
a half-open Pack range from a Box. The active registered client instead opens
downloaded daily Packs through
`trunk.trie().branches().select(branchIds).summaries().pack(bytes, day)`.
Generated summary Definitions are registered separately from event Definitions
and fetched as separate `.rx` resources. Rust reads the summary objects and
evaluates eligible five-minute periods; the host schedules collection and
projects those periods onto the existing event collection plan.

Build the JavaScript/WASM package with `npm run build` in `rust/seeds/js`. Native tests
run with `cargo test --manifest-path rust/seeds/Cargo.toml`. The `ingress_fixture`
example feeds changing typed values through Seed ingress for the WASM tests and
result-delivery benchmark; generated payloads belong in a temporary directory.

### Resource Naming

Use the owning resource followed by a short action. Avoid repeating the resource
in the action name. The Rust database API remains independent of Snoot; host
modules adapt browser storage, workers, networking, and rendering.

| Owner | Action | Responsibility |
| --- | --- | --- |
| `trunk.trie().branches().leaves()` | `read()`, `create()`, `resume()` | Registered per-type, per-version state tries |
| `trunk.trie().branches().summaries()` | `pack()` | Bind daily summary Packs to registered Definitions |
| `trunk.trie().branches().packs()` / `.beds()` | `load()`, `query()` | Selected event Packs and one global query |
| Snoot `time` adapter | `collect()` | Collect the requested timeline window |
| Snoot `planner` adapter | `plan()` | Select ranges from the prepared calendar |
| Snoot `summaries` adapters | `collect()`, `query()` | Summary transport and Rust-backed summary queries |
| Snoot `search` adapter | `query()` | Dispatch the selected event query |

Module names distinguish the two summary adapters: `pil/summary-replication.ts`
owns collection and `search/summaries.ts` owns query orchestration. Native C/WASM
exports retain qualified names because their symbols share a flat ABI namespace.
They are bridge functions, not the developer-facing resource API.

### Cleanup Review — 2026-11-16

Removed the unused Criteria component, nested-PAC stream reader, and disabled
live-summary decoder/cache path. The latter had no production decoder caller;
its collection switch was always false. Removed its obsolete tests alongside
the implementation, retaining the fixture data. Registered daily summary reads
and pruning remain active. HTTP ingestion now feeds the existing Rust summary
accumulators through the registered ingress owner.
The normal client typecheck now rejects unused locals and parameters.

The removed source, its tests, and the affected summary orchestration are saved
in [the cleanup source archive](benchmarks/archive/2026-11-16-client-cleanup.tar.gz).
Existing benchmark results, historical source snapshots, and fixtures remain.

These areas are retained for review; deletion would require more than proving
that a standalone helper has no callers:

| Component | Evidence / Remaining Question |
| --- | --- |
| `go/server/cache/pac_storage.go`, `collect_live.go`, `store_open.go`, `retention.go`, `cleanup.go` | The older Store still calls `FactorPACAllocated` and reads/writes `PILPOD00`. Its preparation, retention, and recovery callers need an ownership audit before removal. The registered Rust read path does not establish that every Store caller is retired. |
| `../snoot/client/services/search/execute.ts`, `pil/wasm.ts`, `search/window-cache.ts` | Registered Definitions dispatch to Rust; an unregistered-data branch still invokes the old host-side evaluator and codec. This cleanup does not certify that fallback or remove it without auditing its callers and fixtures. |
| Object replay in `../snoot/client/services/replay.ts` | Object replay still uses `/write`; this prototype remains separate from the per-type `/leaves` API. |
| `data_seeds::Packs` and `data_seeds::trunk::Packs` | Both are used: the first coordinates supplied Packs, the second binds registered Definitions and owns the scoped coordinator. Keep their namespaces explicit; merging them would change ownership rather than merely shorten a name. |
| `../snoot/client/services/store/browser-harness.ts` | Used by `../snoot/client/scripts/test-store-browser.mjs` through a build input string, although no application import references it. It remains test tooling. |

Historical measurements below describe their recorded implementations, not a
certification that those implementations remain active. Rust/Go build outputs
and the separate browser WASM artifacts also remain: this pass did not establish
which copies can be removed without affecting packaging or old/new comparisons.

## HTTP PIL Ingress

`PUT /definitions` or `POST /definitions` registers a raw RX document with
`Content-Type: application/pil`: RX is itself PIL-encoded, with no JSON envelope.
New registrations provide `Resource-Service`, `Resource-Name`, and
`Resource-Type`; the listener supplies its configured cache. The cache-scoped
`/<cache>/definitions` route uses the same operation. Updates use the same
placement headers and the RX lineage; Rust preserves the latest registered
configuration. `Resource-Split` selects a configured split when applicable.
`Trunk-Branch` remains an optional internal selector. If placement headers
accompany that selector, they must match it.

Rust validates and registers the RX through the shared node transaction. A 204
response returns `Trunk-Branch` for the selected revision and `Location` for
independent RX retrieval at `/<cache>/definitions?file=...`. The separately
replicated `trunk.trie` retains the registration metadata. An update contains the
complete RX lineage, preserving previously published schemas; no version belongs
in the retrieval URL. The registration body limit is 16 MiB, received into the
existing Rust buffer pool. Old Definition-digest negotiation is no longer used.

Event collection uses GET `/packs`; ingestion uses POST `/seeds`.
`/beds` is reserved for server-side query execution and is not implemented yet.
The object-history `/collections` resource remains separate.

`POST /<cache>/<service>/<resource>/<type>/seeds` accepts one complete,
uncompressed PIL as `Content-Type: application/pil`. `Trunk-Branch` selects the
registered Definition. The request body has no JSON envelope or recovery prefix.
Rust reads and packs the PIL; the HTTP adapter transfers bytes and performs I/O.

HTTP 204 acknowledges a synchronized recovery append. Current-hour PILs are
spooled in `<type>.seeds`; rollover retains the previous hour as
`<type>.<hour>.pack` for its fifteen-minute straggler window. Recovery checks its
own framing/checksum, discards an incomplete final append, and rejects a complete
corrupt record. A failed write stops admission on that resource until recovery.
The commit scheduler prepares the sealed Pack and index in Rust, persists a redo
checkpoint, flushes `.packs` and `.indx`, then atomically publishes `.time` last.
Restart replays unfinished publication and skips already-published PILs.

GET `/packs` retains the existing sealed Pack stream and appends selected open
Packs. Open Packs stay in Rust memory on the client, never in durable BEDS.
Rust queries stored readers and live Packs together with one ordering and limit.
Pagination detects changes to an open Pack and requires restarting the search.
An empty Time response retains the origin header without certifying any hour.

The initial limits are one MiB per PIL, 64 MiB of retained input per resource,
and two open hours. The input limit is not a total heap measurement. HTTP retry
is not idempotent: an uncertain acknowledgement can duplicate an event. Admission retains the full PIL for live queries, spooling and annotated summary
accumulation. At seal, registered state paths can move repeated values into
the per-Definition leaf root.

### Query Telemetry

The server registers the Rust-authored `log.query` Definition (`log-query.rx`)
under service `trie-trunk`, resource `queries`, and type `query`.
Snoot submits completed, failed, and cancelled searches
through that same PIL endpoint. Autocomplete previews do not submit events.
The worker encodes the observations with Rust before POSTing them.

Query observations carry `operation: "execute"` and OpenTelemetry-mapped `level`
(INFO/9 for completion, WARN/13 for cancellation, ERROR/17 for failure).
Stored timing fields use microseconds: `duration` measures submission through
presentation opportunity. The `@checkpoints` array contains ordered `{name, start, duration}` spans in
microseconds. `start` is relative to submission and remains absent when unknown.
The annotation enables a colored timeline popup; overlapping spans are never
summed to calculate total elapsed time.
`worker` covers worker execution; `before_collection`,
`collection`, and `after_collection` divide that execution at collection
boundaries. `presentation` covers receipt through the next two animation frames
in a visible tab. This is a paint opportunity, not a browser render-completion
measurement. Hidden tabs finish at receipt. Unavailable timings remain absent.
The telemetry queue is bounded to eight submissions and reports failed delivery
in the console; it does not retry ambiguously acknowledged events.

Collection diagnostics separate response headers, index response headers, leaf
retrieval, index import, source preparation, and Pack publication. These spans
can overlap; their sum is not the end-to-end duration. Source preparation includes
publishing required queued dependencies. The older `responseWaitMs` observation
also waits for indexes and must not be interpreted as network latency alone.
Current measurements and limitations are recorded in
[Collection And Publication](research/benchmarks/collection-publication-2026-11-17/README.md).
The subsequent [Local Publication I/O](research/benchmarks/publication-io-2026-11-17/README.md)
pass separates socket service time from local reads, writes, flushes, and commit
publication. The node retains directory handles while reading commit roots
afresh. Unpublished layout/Time revisions are written and flushed concurrently;
one atomic root replacement makes them visible only after both succeed.
The [Worker Dispatch Comparison](research/benchmarks/worker-dispatch-2026-11-17/README.md)
separates posting, node receipt, service handoff, execution, and result delivery.
Native tests can compare the default MessagePort path with opt-in host-tab
Worker request and reply paths using `--dispatch`; all retain one node and writer.

## Query Planner

### SQL Queries

Snoot’s SQL tab supports `SELECT *`, selected fields and aliases, comparisons,
Boolean expressions, `IS NULL` / `IS NOT NULL`, and event timestamp ordering.
`COUNT`, `SUM`, `MIN`, `MAX`, and `AVG` return one row across matching events or one row per explicit `GROUP BY` key. Joins,
subqueries, window functions, `HAVING`, and SQL pagination are not yet supported.
Empty totals return zero for `COUNT` and NULL for the other aggregates. Empty grouped queries return no rows.

Autocomplete suggests supported clauses, fields, aggregates, and values as you
type. Drag the Pipeline, Builder, and SQL tabs to set their order; the first tab
is the default for new queries. Alt+Left/Right Arrow also moves a focused tab.

```sql
SELECT duration, COUNT(*) AS total, AVG(amount) AS average
FROM events
GROUP BY duration
```

Event reads use the existing search cursor and pagination. For totals and
breakdowns, the shared Rust planner combines complete five-minute periods when
their statistics prove the result. It reads matching events for partial periods,
missing coverage, and groups or predicates that need individual values. The
10,000-group limit applies across both paths.

Query the event dataset directly. The planner uses its pre-aggregated statistics
internally. `AVG(field)` combines sums and non-null observation counts across
the selected events. Integer accumulation checks for overflow, and averages and
floating sums use floating point.

SQL comparisons use stored numeric units. Explicit null and absent remain distinct:
`IS NULL` matches null only; comparisons with either null or absent yield unknown.
Pipeline and Builder retain their existing predicate semantics. Unsupported syntax
conversions report an error without replacing the query text.

All three editors share one query model and execution path. Pipeline adds
`select ... | group by ...`; Builder adds `.select(...).group_by(...)`, with the
same selection expressions as SQL. Parsing and summary reduction run in Rust,
through `rust/seeds/language-wasm`; the storage adapter uses existing decoded
summary data and preserves the current retrieval, cache, and replication paths.
No codec or storage format changes are part of the SQL tab.

Build the query runtime with `npm run wasm:query` in `client` (also included in
its normal WASM build). Query tests reuse server-ingestion fixtures and verify
that summary execution does not request events or indexes.

### Count Queries

`events | count` and `events.query().count()` count events in the selected
scope and time window. For eligible complete windows, the planner uses the
existing pre-aggregated retrieval path. Predicates, partial-hour edges, live tails,
or unavailable pre-aggregates retain event execution when required. Users never
select the underlying optimization as a dataset. The result is available in the
existing result metadata's `count`, without event rows. These are Local Search operations.

`../snoot/client/scripts/benchmark-summary-query.mjs` compares the two text queries through
calendar planning and `ViewCache.query`, checking identical counts and storage
requests. It uses existing ingestion fixtures with an in-memory storage backend
and fixture HTTP responses; its times exclude physical disk, network, and UI.


The planner chooses how to answer a query without changing its meaning. Develop
alternative plans explicitly, compare them on identical inputs, and keep the
evidence with the code. A fast result with different rows is a failed experiment.
Estimates may choose an algorithm; they cannot establish that data may be skipped.

This section describes the new `rust/seeds` query layer. It does not certify or
replace the existing Snoot planner. Pill Packer supplies byte access and container
addressing. Seed Packs evaluate predicates; PACKS/BEDS coordinate the result across
Packs. Generic Boxes have no knowledge of query limits, timestamps, or joins.

### Implementation Status

| Capability | Current State | Next Experiment |
| --- | --- | --- |
| Definition binding | Field names compile to ordinals per registered Definition | Measure reuse across queries with different parameters |
| Pack filtering | Comparisons, presence/null checks, Boolean composition | Predicate ordering and selected-field access, with identical error semantics |
| Unordered results | Streaming cursor, global offset/limit, cancellation | Fetch sizes and bounded scheduling |
| Ordered results | One sort field; bounded global top-K, requiring a limit | Compare against the development full-sort reference |
| Result delivery | Projected views or materialized objects | Measure actual field consumption and retained backing memory |
| Development plans | Explicit strategy builder, explanation, paired A/B reports | Add one proven alternative at a time |
| Summary pruning | Rust evaluates registered summary predicates; the host projects eligible periods onto collection ranges | Preserve coverage and unknown-period handling when adding more pruning strategies |
| Sorted-stream merge, parallel workers, spilling | Not implemented | Ordering guarantees, memory budgets, cancellation |
| Summary aggregates | Shared Rust reduction over supported summary classes | Extend supported operations with independent reference results |
| Event joins | Two-input hash equijoin with global output limit and budgeted build side | Compare build-side choice, indexed lookup and merge alternatives |
| Stalk columns | In-memory column access | Measure conversion and reuse across analytics |
| Automatic cost-based plan selection | Not implemented | Collect calibrated costs and held-out evidence first |

### Query Meaning Before Execution Strategy

Keep three separate concerns: the logical query defines the answer; the physical
plan chooses operators; the execution policy assigns memory, concurrency, and
delivery batches. Changing a delivery batch size must not change query results.

The current snapshot fixes source order and member ordinals. Filters preserve
duplicate rows. Absent and null remain distinct; comparisons can be unknown, and
only true predicates select rows. Mixed integer/float comparison must not round
large integers through `f64`. Current text ordering uses Rust string comparison,
not locale collation. Null and absent sort after values in either direction;
NaN sort keys are rejected. Equal keys use source ordinal then member ordinal.
That tie rule is repeatable within a snapshot, not a persistent event identity.

`limit(10)` means ten rows after global ordering and offset, across every selected
Pack. `fetch(10)` asks a cursor for up to ten more rows. It is not another query
limit. Without an order, the current implementation returns source/member scan
order; scheduling changes must preserve that contract or explicitly revise it.

For an unsorted dataset, a global top-K heap retains `offset + limit` candidates
but still inspects all qualifying rows. For genuinely sorted streams, a future
merge can request the next candidate only from the winning stream. A local top-K
can feed a global top-K only when both use the same total ordering and no later
operator changes eligibility or multiplicity. Joins, distinct, and aggregation
can invalidate a premature limit. Never return the first workers to finish as
the answer to an ordered query.

Before adding an operator, specify its input/output types, duplicate behavior,
null/absent behavior, ordering, overflow/error behavior, and cancellation rules.
For replicated inputs, distinguish an empty range from an unavailable range.
Missing coverage cannot become an empty successful answer without an explicit
partial-result contract. These replication semantics are integration work, not
properties supplied by the current in-memory `Sources` catalog.

### Properties And Estimates

The registered event reader can seek the existing hour index for integer
equality and range comparisons on the annotated millisecond timestamp. It
intersects those bounds with the selected calendar and summary periods. A
timestamp-only predicate can prove exact row ordinals. With an additional
conjunct, the bounded rows are candidates for the leaf index or the PAC
evaluator. This optimization requires all queried Definitions to agree on
the timestamp bounds. An index-free source evaluates the same predicate
against its records. The cached prescription holds the bound comparison;
each cursor obtains its row ordinals from the selected publication.

A UUIDv7 equality predicate on an `@event` or `@log` identity can seek the
UUID's millisecond in that index. Events can share a millisecond, so the
reader compares all 16 logical UUID bytes for every candidate. An `@object`
identity compares its stored 16 bytes directly, including when its value
happens to be a UUIDv7.

| Information | May Establish Correctness? | Use |
| --- | --- | --- |
| Registered field category, width, ordinal | Yes, within that Definition | Bind codec access plans; never guess from field names |
| Verified sort order, comparator, coverage, snapshot generation | Yes | Enable ordered merge or safe early termination |
| Exact index or min/max bounds with matching semantics and coverage | Yes | Reject impossible candidates, retain residual checks where needed |
| Sampled selectivity, distinct counts, histograms, heavy hitters | No | Estimate scan work, join size, and memory |
| Correlation and conditional selectivity | No | Avoid multiplying unrelated marginal estimates blindly |
| Observed field width, compression ratio, cache residency | No | Estimate bytes, decode work, and materialization cost |

Each statistic needs scope, collection method, sample size, age, and source
generation. Unknown is not zero. Statistics belong to the data they describe;
one resource's distribution must not become a global assumption. Correlated
predicates and skewed join keys deserve separate experiments from uniform data.
PostgreSQL's [planner statistics documentation](https://www.postgresql.org/docs/current/planner-stats.html)
provides concrete examples of single-column and multivariate statistics.

Summary pruning additionally requires that the aggregate exists for the queried
field and that its bounds are sound for the predicate and covered time window.
A partial interval can be rejected using a containing interval's safe bounds;
it cannot generally be answered exactly from that larger interval's aggregate.
Incomplete tails and changed aggregate subscriptions need explicit coverage.
Use ingress-produced summaries; query planning must not silently reconstruct
them by aggregating source records and call that summary lookup.

### Proposed Operator Tree For Joins And Aggregates

The registered Seed reader already has a per-Definition expression program, a
bounded cursor and a global limit. The next architectural step is a typed
relational plan above that reader. The proposed public entry point is
`trunk.trie().branches().query()`. SQL, Pipeline and Builder would supply the
same Rust planner. The existing wire requests can remain adapters to it.

| Responsibility | Proposed Rust owner | Contract |
| --- | --- | --- |
| Bind | Query binder at the registered branch scope | Resolve each input through the taxonomy. Identify fields by input and logical field identity, with Definition-specific ordinal accessors. Resolve aliases, casts, units and missing-field semantics here. |
| Describe the answer | Logical plan | A scan names a source, a filter or aggregate has one input, and a join has two. Each node exposes its output fields. Join kind, grouping keys, global order and limit are explicit. |
| Choose execution | Optimizer and physical plan | Use estimates to rank eligible implementations. Track proven order, exact predicate coverage and required fields separately. A candidate-only scan keeps a residual filter. Summary eligibility requires sufficient coverage and compatible aggregate state. |
| Run | Per-query operator state | Keep scan windows, join tables and group accumulators in the running query. Operators consume bounded batches under the cursor's work, memory and cancellation budgets. Store-backed scans keep the existing PAC reader, row references and lazy field access. |

The plan tree describes operator boundaries. A filtered and projected scan
can still execute as one native loop. Cached plans contain immutable
instructions. Reader leases, current collection readiness and partial output
belong to the execution against its chosen snapshot.

For example, a count of events joined on a request ID must count the matched
pairs. Two rows on one side and three on the other produce six. Independent
branch counts cannot supply the join-key multiplicities. A grouped average
over a single branch can instead merge summary sums and non-null counts when
the filters, grouping and coverage permit it. Both cases use the same logical
aggregate with different eligible physical inputs.

The event join now binds two filtered scans and runs a right-build hash join.
Its regression tests cover duplicate fanout, null and absent keys, incompatible
types, cancellation, memory exhaustion and the global output limit. The public
GitHub event replay compares every output pair with an independent reference.
Cost-based join selection still needs measurements of alternative operators.
Plan explanations should record the chosen access path and the reason a summary
or index could only narrow candidates.

This proposal draws on [DataFusion's logical and physical optimizer rules](https://datafusion.apache.org/library-user-guide/query-optimizer.html),
[its filter pushdown contract](https://docs.rs/datafusion/latest/datafusion/datasource/trait.TableProvider.html),
[its mergeable aggregate state](https://docs.rs/datafusion/latest/datafusion/logical_expr/trait.Accumulator.html),
and [PostgreSQL's separation of candidate paths from executable plans](https://www.postgresql.org/docs/current/planner-optimizer.html).

### Event Joins From The CLI

`trunk beds query --sql` accepts one `INNER JOIN` or `LEFT [OUTER] JOIN` between registered event sources. `events` names the selected scope. A taxonomy-qualified name selects another event type. Both inputs share the command’s half-open time window.

```sql
SELECT i.source_id AS activity, c.source_id AS comment
FROM events AS i
LEFT JOIN events AS c
  ON i.repo = c.repo AND i.issue = c.issue
  AND c.event_type = 'IssueCommentEvent'
WHERE i.event_type = 'IssuesEvent'
```

The binder checks key compatibility across registered Definitions. Each input uses its existing filtered reader. The executor builds a hash table from the right input and streams matching pairs, carrying duplicate matches across pages. An absent or null key cannot match. A left join emits one row with null right-hand columns for each unmatched left row. The node enforces its shared memory budget and the CLI’s global row limit.

Use qualified fields in projections and predicates. Use `AS` to disambiguate repeated column names, or `SELECT *` for alias-qualified output names. The first implementation supports conjunctive comparisons. In a left join, right-input literal filters belong in `ON` and left-input filters in `WHERE`. Grouping and ordering joined rows, additional joins, and object inputs remain future work.

### Optimization Paths

| Operator | Alternatives To Evaluate | Evidence Required |
| --- | --- | --- |
| Scan/filter | Sequential field access, exact index candidates, bitmap intersection, summary rejection | Selectivity, candidates examined, fields decoded, bytes touched, coverage |
| Projection | Delay unused fields; retain filter/sort/join keys until consumed | Fields and objects materialized; backing memory retained |
| Order/limit | Bounded top-K, proven ordered merge, full sort, later external sort | K, matching rows, key widths, ordering proof, memory and spill bytes |
| Join | Hash join, merge join, indexed lookup; select build side | Input cardinalities, distinct keys, skew, duplicate fanout, memory |
| Aggregate | Streaming state, hash groups, ordered groups, compatible rollups | Group count, merge algebra, state bytes, precision/error bounds |
| Repeated analytics | Filter/project once into Stalk columns | Conversion cost, reuse count, column bytes, repeated-query savings |

Predicate reordering requires compatible purity and error semantics. Projection
pushdown must retain fields needed later. Outer-join predicate pushdown requires
special treatment of unmatched rows. Join planning must preserve duplicate
multiplicity and define equality for missing/null keys; a convenient hash lookup
is not sufficient to establish SQL join semantics.

Future event-to-object joins take their meaning from the criteria. An object
ID alone addresses current state. Adding a revision ID selects that revision.
An as-of lookup needs an explicit time criterion. A covering object index
could answer any of these only if it covers the requested fields and revision
semantics. Event-to-event joins can be developed independently of PODS.

Rollups have different algebra. Counts and sums combine; means combine through
sum and count, not means alone. Min/max combine over compatible sets. Exact
medians and percentiles do not combine from percentile values alone: use raw
values or an explicitly chosen mergeable sketch with stated error. Variance
requires suitable mergeable state. Define overflow, units, null handling, and
group identities before introducing any aggregate rewrite. Arbitrary app
annotations do not alter codec behavior.

A Stalk is a future analytical representation, not a change to PIL. Keep only
the selected dimensions needed for repeated work, preserve row identity through
selection/permutation vectors, and account for construction time and retained
memory. Whether conversion pays off depends on reuse; one narrow filter is a
different workload from twenty charts over the same selected dataset.

### Cost Models

Begin with a small set of legal alternatives, not an exhaustive search over
every operator combination. Keep first-result latency, complete-result latency,
and retained/peak memory as separate objectives. A plan that wins one can lose
another. Set a planning-time budget too: a better plan is not useful if choosing
it costs more than executing the query.

Calibrate costs from measured operations: Pack opens and decompression, candidates
visited, fields accessed, bytes decoded/copied, sort comparisons, retained keys,
join probes, group updates, and output materialization. Use row count together
with width and distribution; ten wide strings are not ten small integers. Shared
costs such as one decompression or one Definition binding must be charged once.
An additive estimate is a starting hypothesis, not a claim that cache behavior,
parallelism, or allocation costs are independent.

Compare estimates with actual cardinalities and operator work after execution.
Investigate estimation error separately from an inefficient physical operator.
Do not optimize estimation error alone when a different error has no effect on
plan choice. Learn coefficients on development workloads and evaluate plan
selection on held-out workloads. Learned estimators may eventually advise the
planner; they must not replace eligibility proofs or the safe execution path.
No cost estimator or learned selector is implemented in the current lab.

### Development Plan Builder

Enable `planner-lab` to build physical models without changing normal execution
defaults. The feature adds a size-capped full-sort reference and instrumentation;
neither is present in normal builds. It does not install an automatic optimizer.

```rust
use data_seeds::{Direction, Query};
use data_seeds::planner_lab::{Experiment, PlanModel, Selection};

let query = Query::new()
    .select(["sequence", "score"])
    .order_by("score", Direction::Descending)
    .limit(10);

let reference = PlanModel::new("full-sort")
    .selection(Selection::FullSortReference)
    .max_candidates(100_000);
let candidate = PlanModel::new("top-k")
    .selection(Selection::BoundedTopK)
    .max_candidates(10);

let explanation = candidate.explain(&query)?;
let report = Experiment::new("ordered-ten", query, reference, candidate)
    .expected(expected_rows) // Computed from original ingress events.
    .warmups(2)
    .pairs(12)
    .seed(42)
    .run(&sources)?;
```

`max_candidates` bounds retained candidate count, not bytes: string keys can be
wide. Exceeding this experimental budget returns an error, never truncated
results. `batch_rows` controls delivery batches. Unordered experiments use
`Selection::Streaming`. Unsupported strategy/query combinations are rejected.
New physical operators must be executable and explained before joining this API;
there are no placeholder join or aggregate switches.

For registered readers, the same physical model also selects which indexes
may evaluate the predicate. Models round-trip as JSON. For example:

```json
{"name":"time-leaves-top10","access":"TimeAndLeaves","selection":"BoundedTopK","batch_rows":256,"max_candidates":10}
```

`Ranking::run(&reader, &query, &plans, &expected_rows, rounds, seed)` runs
each model on the same pinned reader. Access can be `Scan`, `Time`, `Leaves`,
or `TimeAndLeaves`. Every path respects the source window, restores indexed
fields, and evaluates any residual predicate. The access setting permits a
pushdown when the index and predicate support it. Each model gets one warmup.
Measured rounds rotate execution positions and check the complete ordered
result against the supplied oracle. Scores rank by median elapsed time and
retain p95, per-run phase timings, rows scanned and peak sort candidates.
Timing starts at cursor creation and includes materializing the returned page.

Reproduce the eight-plan comparison on the two-hour in-memory storage fixture
and save its scores and raw samples:

```sh
SEED_PLAN_REPORT=/tmp/seed-plans.json cargo test --offline --release --manifest-path rust/seeds/Cargo.toml --features planner-lab,query-language --test reader_index rank_hour_indexes -- --ignored --nocapture
```

Run the complete example, including typed Seed ingestion and independent expected
results, with:

```sh
cargo run --manifest-path rust/seeds/Cargo.toml --release --features planner-lab --example planner_lab -- /tmp/planner-lab.json
cargo test --manifest-path rust/seeds/Cargo.toml --release --features planner-lab
```

The example covers selective uniform values, a dominant repeated key, correlated
predicates, wide text sort keys, and streaming delivery. It creates every Pack
through Seed ingress and keeps original events for the oracle. These are
development fixtures, not a representative workload certification. Reports retain
every timing pair, exact input fingerprints, query/model descriptions, source
fingerprint scope, toolchain, and target. No payload fixtures need to remain on disk.

### Measurement Contract

Each experiment keeps one logical query, input snapshot, and output consumption
contract. Change one physical decision at a time. Compare every returned value,
row order, and multiplicity outside the timed region. Supply an independent oracle
from original ingress events; without one, the report says differential agreement
only, because shared decoder or comparator defects can affect both plans.

The current timer starts before fresh query binding and ends after all selected
native objects have been produced. It separates binding, cursor fetch, output
materialization, and remaining loop work, and records first output. Temporary
batches are disposed inside that interval. Final output/cursor destruction,
verification, ingress, Pack opening/decompression, runtime upload, and rendering
are outside it. These reports measure prepared native queries, not end-to-end
application latency or lazy-view delivery. Those require separate benchmarks
with their own explicit boundaries and actual field consumption.

Record work as well as time: scanned rows, predicate matches, output rows, peak
candidate count, encoded input bytes, and unique retained backing capacity are
currently reported. Peak allocator usage, RSS, output heap, field-decode counts,
and hardware counters are not yet measured. Do not relabel candidate counts as
memory usage. A view of one row can retain a large Box allocation. Future parallel
plans must budget concurrency and retained buffers, not just per-worker state.

Run balanced, shuffled A/B and B/A pairs with reproducible seeds. Record warmups;
fresh cursors do not make CPU, OS, or Pack caches cold. The harness reports a
paired median speedup and bootstrap interval conditional on that fixture and
process. Few runs do not establish tail latency or general superiority. Timers
and instrumentation themselves have costs; compare identically instrumented
plans, then remeasure a selected implementation without development instrumentation.

Before promotion, predeclare the primary metric and tolerable regressions, repeat
in independent processes, and use held-out distributions/seeds and representative
hardware. Keep individual workload results; an average can conceal a serious
regression. Testing many variants and reporting only the best creates selection
bias. Retain the failed/neutral experiments and rerun the winner on unseen cases.

Expand coverage before general claims: empty/all/no matches, null/absent values,
wide/high-cardinality data, duplicates, skew, correlated fields, reordered/mixed
Definitions, best rows in the last Pack, tiny versus large Packs, common-only
Packs, compressed inputs, shared Box ranges, unavailable replicated ranges, and
changing limits/offsets. New joins need fanout and unmatched-row oracles; new
aggregates need independently computed reference values and error tolerances.

### Plan Reuse And Promotion

Cache immutable binding/access plans separately from reader cursors and result
data. A reusable physical plan must identify the logical query, relevant
parameters, Definition structure, application semantics, source generation, and
required physical properties. Annotation edits can leave a Definition digest
unchanged, so application interpretation needs its own invalidation signal.
Process-local Definition pointer reuse is not a persistent cache identity.
Statistics changes may affect plan choice without invalidating correctness;
lost sort/coverage guarantees do invalidate an optimization that depends on them.

Every proposed optimization should record:

| Review Item | Required Answer |
| --- | --- |
| Eligibility | Which properties prove this rewrite preserves results? |
| Hypothesis | Which operations, allocations, copies, or reads disappear? |
| Resources | What is bounded; what happens on skew or underestimated size? |
| Fallback | How is correctness preserved if the prerequisite fails? |
| Validation | Which independent oracle and adversarial cases passed? |
| Evidence | Exact source/artifact, inputs, raw measurements, cache state, consumption boundary |
| Decision | Keep, revise, or reject; workload limits and remaining unknowns |

The harness does not promote plans automatically. Preserve the executable
baseline and raw report. Include path-dependency codec source/artifact identity,
not just a Cargo lockfile, in any promotion evidence. Do not add silent algorithm
fallbacks that obscure which plan ran; explain and measure any fallback path.

For research grounding, PostgreSQL's [EXPLAIN guide](https://www.postgresql.org/docs/current/using-explain.html)
shows why estimated costs, actual rows, loops, and execution time are distinct.
[How Good Are Query Optimizers, Really?](https://vldb.org/pvldb/vol9/p204-leis.pdf)
motivates testing cardinality estimation and plan selection separately.
[Cardinality Estimation in DBMS: A Comprehensive Benchmark Evaluation](https://arxiv.org/abs/2109.05877)
examines why estimation error alone is insufficient to judge query performance.
Our application of those lessons is to pair operator-level evidence with complete
query outcomes, rather than optimize a prediction score in isolation.

## Pack and Replication Design

[Pill Packer](../pill-packer/README.md#pack-api) owns PIL/PAC framing,
named headers, optional embedded member directories, fixed-width validation,
and explicit compression. Trie-Trunk owns grouping, identity, storage placement,
replication, and retention. Ordinary event members remain variable-width.

`seed.sprout()` materializes present fields for rendering; `seed.sprout(fields)`
accepts field names or zero-based Definition ordinals, including mixed selections.
Ordinals do not shift when fields are absent. Explicit selections retain missing
values as `undefined`; null stays null. Seed time, leaf, shared-state, and PAC common
values resolve before absence is reported. Reuse the selection array across rows.
The bulk reader shares Pill Packer's layout projection, without assembling a PIL
or serializing JSON; byte values remain borrowed. `pill.crush()` exposes the generic
equivalent without Seed application semantics.

The development branch index uses six-byte rows: four timestamp bytes and a
two-byte state leaf. Record position supplies the ordinal; payload boundaries
come from the PAC directory. The former 16-byte index and the entropy-preserving
experiment are historical baselines, not current layout descriptions.

Registry/Definition discovery initializes the workspace. Calendar changes refresh
scoped timelines, show hourly counts, and prepare coalesced resource ranges before
Search. The planner consumes that prepared calendar; collectors neither retrieve
timelines nor rediscover gaps. Sealed collected Packs become query-visible after
their dependency and `.beds` publication barriers succeed. Unsealed live Packs
remain resident. A branch waits for its own queued dependencies; unrelated
branches do not need to publish first. Same-branch imports wait through Pack
publication and resident invalidation, preserving newly queued data. Fast dashboards
replicate summary PACs separately. Tantivy remains an explicit server comparison;
production client Tantivy replication and execution are deferred.

After Time selects the hours, leaf-backed predicates can screen the existing
six-byte hour-index rows before requesting event payloads. An hour with no
candidates needs no PAC read when the registered roots agree. Reused leaf IDs
with different meanings can require PAC version metadata. The separate proposed
leaf-to-hour occurrence directory remains future work.

## Architecture

- **Go** manages HTTP connections, authentication, configuration, scheduling,
  sealing, retention, and response assembly.
- **Rust** owns resident PIL buffers, live identity directories, Tantivy indexes,
  and native search execution. Go calls Rust through an in-process C ABI.
- **Snoot** uses Solid, Unovis, and ECharts. A SharedWorker coordinates browser storage,
  PIL/WASM decoding, query execution, and cached complete datasets.
- **Data seeds** implement PIL’s typed positional records. **Seed packs** implement
  PIL PACs, grouping data seeds under a shared Definition with common values and
  optional compression.

This is not yet a fully native record engine. Go still allocates projection,
query-result, and sealing metadata. Measure native memory and process RSS along
with Go allocations; moving ownership alone does not establish a latency win.
Blind CBOR and JSON inputs are decoded at ingestion. Definitions, event payloads, controls, metadata, and browser state use PIL.

### Transaction Logging

The intended API/event strategy emits one outcome seed per transaction: `info`
for success or `error` for failure. Supplemental `warn` seeds retain their level
but use the debug storage container, alongside `debug`; `trace` has its own
container. Access remains a separate `read`/`write` field. The Go logger routes
warnings accordingly and classifies HTTP failures, including 4xx and cancellation,
as error outcomes. Cross-seed correlation and enforcement of the complete
outcome-plus-diagnostics strategy remain to be implemented.

### Provider Definitions And Transforms

[`schemas/logs/catalog.json`](schemas/logs/catalog.json) lists the runtime `.rx`
Definitions for Log, HTTP, Database, Messaging, Security, Job, Task, Workflow,
and System. Names are `<shape>_<provider>.rx`, for example
`log-http_trietrunk.rx`, `log-http_opentelemetry.rx`, and `log-http_datadog.rx`.
CDDL source files and generated `.cddl.json` are authoring inputs only. The build
tool owns those source paths; ingestion loads `.rx` containers.

Build from `go/` with `go run ./cmd/build-log-definitions --rx /path/to/current/rx`.
The OpenTelemetry profile describes one log record, not an OTLP export envelope.
The Datadog profile explicitly accepts an integer millisecond timestamp. Job,
task, workflow and security-specific additions are application profiles, not a
claim that OpenTelemetry defines all of those event categories.

`definition.meta` contains one PAC per exact Definition, with one metadata PIL
per field ordinal. `synonym.map` contains a PAC for each source/destination RX
pair. Rows contain only a source field path, destination field ordinal, cast,
explicit cast arguments and precedence. Types and constraints live exclusively
in the paired RX files. Exact source, destination and mapping-schema digests
are PAC application headers. The existing `registry.map` addresses each complete
PAC by byte range under `DefinitionResources`, keyed by RX digest. It is a bundle
registry, not a replacement for an active server registry.

`presentation.json` authors display metadata; `synonyms.json` authors bindings.
Known codes are stored once. `metadata_label` consumes an accompanying label
only when the destination metadata can reproduce it exactly. Unique labels can
resolve to codes; ambiguous, conflicting or custom labels remain additional
fields. Database labels use their engine context. Unknown numeric codes remain
valid when allowed by the RX. No HTTP-specific field names are hard-coded in the
transform executor.

The existing resource `transforms.cfg` operations remain unchanged. A provider
binding can be added to that configuration:

```toml
target = "<exact bound resource RX digest>"
[provider]
bundle = "/absolute/path/to/schemas/logs"
definition = "log-http_trietrunk"
```

Loading verifies that binding the authored destination to the resource produces
its exact storage RX. Plans bind field ordinals once and reuse scan slots.
Compatible PIL field bytes are borrowed. Numeric casts reuse scalar buffers.
The current cast vocabulary is `identity`, `decimal_to_integer`, `scale_exact`,
and `metadata_label`. `scale_exact` rejects precision loss and overflow; it does
not silently round timestamps. Higher explicit priority selects an alternative,
retaining a different lower-priority value in additional fields. Equal-priority
conflicts reject the record. Missing fields stay absent; the destination codec
enforces required fields and constraints. This is a finite cast contract, not
executable script embedded in the map.

POST provider records to `/ingest?tap=<cache>/<service>/<resource.vN>/<branch>`
with `Definition-Version` set to the source RX digest. A concrete destination is
required. `application/pil` uses the existing leased Rust-buffer stream, followed
by a single destination encode into retained native memory. It does not expand
records to JSON. Explicitly selected JSON, NDJSON and CBOR provider adapters bind
the external record once through the source codec before that same transform
path; their source conversion allocates temporary codec output. They are not a
zero-copy substitute for PIL ingress. Blind ingestion without a provider digest
retains its existing behavior. Batch acknowledgement preserves the durable prefix;
rejected PIL records retain their original source RX identity in the audit.

Generating this bundle does not replace the running logger's Definition or
rewrite existing data. Install provider bindings on the resources that should
accept them. Metadata readers prepare ordinal and value lookups once, avoiding
per-cell PAC decoding. Verify with `go test ./internal/definitionmeta ./internal/seedtransform ./server/cache`
from `go/`, and
`node --experimental-strip-types --test services/definition-meta.test.mjs` from
`client/`.

### Data Lifecycle

Data seeds are events; seed packs group those events. PACKS means **Packed Array
Container Kompressed Storage**: a store of sealed Pack containers. BEDS means
**Browsable Event Datastore**: event data collected for querying and traversal.
These are storage roles, independent of deployment: a server can query Beds,
and a desktop node can use both. Both use Pill Packer Boxes and the same Seed
query tooling.

| Stage | File | Purpose |
| --- | --- | --- |
| Seeds — Spooled Epoch Event Data Storage | `.seeds` | Ingestion spool and recovery persistence, with recent events in memory for live queries |
| Pack — Packed Array Container Kompression | `.pack` | Hourly staging. Accepts late arrivals for 15 minutes and protects recovery until the hour is sealed and durably published |
| Packs — Packed Array Container Kompressed Storage | `.packs` | Sealed seed packs, typically at the origin; existing branch compression policies apply |
| Beds — Browsable Event Datastore | `.beds` | Queryable event datastore, selectively populated from collected packs; a missing hour remains unknown |
| Stalks — Sparse Telemetry And Lineage Kolumnar Storage | `.stalks` (planned) | Analytics representations using Tantivy's columnar crate |
| Pods — Packed Object Datastore | `.pods` (reserved) | Future branch-scoped object datastore, intended to own the collection index and revision history; physical organization deferred |
| Summaries | `.summ` | 5-minute columnar aggregates, daily compressed summary packets, and calendar rollups |

Stalks is the name for the Tantivy-backed analytics storage going forward.
Current browser analytics use precomputed summaries; integrating Stalks is future
work. Tantivy remains the underlying library name in implementation references.
Lineage is a separate design direction: definition-declared relationships connecting
resources, such as payments and invoices, through identity and time. Relationship
traversal and broader server-side analysis are not implemented yet.

The standalone collection-index prototype remains available in
`rust/uuid-index`. The branch-scoped `.pods()` API and `.pods` datastore are the
approved home for object and revision storage and metadata. Integrating the
prototype's collection index and revision history there remains a design task;
its existing persistence and recovery behavior are preserved in the meantime.

Research retrieves complete matching seed packs. Object history and replay use
separate access to individual data seeds. A missing client hour is unknown, not
an empty result. Only complete hours enter durable coverage. Summary requests
remain independent of event paging; paging must not reduce full-range counts.

“Event” still describes the meaning of a data seed. Index records and summary
packets are separate structures; they are not renamed to data seeds or seed packs.

```text
caches/
  trunk.conf                  # Snoot Trunk settings and inherited root policy
  trunk.trie                  # taxonomy and registrations, stable 16-bit Branch IDs
  definitions/
    doc / rx                  # local copies of the existing tools
    <digest>.rx               # shared immutable Definitions
    producer-<digest>.rx      # negotiated producer Definitions
  <cache>/
    <cache>.conf              # only explicit cache overrides
    <service>/
      <service>.conf          # only explicit service overrides
      <resource>/
        <type>.v<major>.packs # origin Pack store
        <type>.v<major>.beds  # browsable replica or local datastore
        <type>.v<major>.time  # generation-local timeline and addresses
        <type>.v<major>.indx  # generation-local record indexes
        <type>.v<major>.summ  # generation-local summaries
        <type>.v<major>.leaf  # minor-version state roots
```

A major revision starts a separate physical generation; minor revisions share
that generation and may coexist in mixed PACs. HTTP splits add their configured
name before the version suffix (for example `http.error.v1.packs`) and share
`http.v1.leaf`. Old and new majors can receive events in the same hour. Their
commit, recovery, timeline and summary state remain independent.

These files remain one logical resource in the taxonomy and user interface.
Discovery selects every retained major unless restricted, and the planner
collects each generation once. Raw results preserve each record's Definition;
version labels never authorize implicit casts or joins. Operations requiring a
unified type must satisfy the query engine's type checks. Physical path selection
comes from `Route::storage()`, while `Route::source()` stays version-free.
Snoot's existing private OPFS cache still uses hashed source directories and an
`events.beds` basename within each directory. Those cache identities include the
registered Definition and physical generation, so majors are isolated there too;
they are not exported datastore filenames.

`<type>.v<major>.leaf` belongs to one type and major. HTTP splits share that
file; different types and majors use separate files. Each Definition version occupies one
contiguous root section. The root declares its Definition once, followed by
framed node batches. Appending a successor freezes its predecessor without
moving existing nodes. Leaf IDs remain two-byte ordinals local to their root.

State tooling is reached through
`trunk.trie().branches().select([branch]).leaves()`. `read(bytes)` opens the
selected version, `inspect(bytes)` checks incoming integrity before storage,
and `create()` / `resume(bytes)` provide the scoped append builder. Registering
a successor and publishing emits its root, nodes, and replacement footer as one
snapshot. For hourly commits, `prepare(branch, nodes)` produces only the new
node/root tail and replacement footer. The host writes that tail at its supplied
offset, truncates the old footer, syncs, then calls `accept(commit)`. Dropping a
prepared commit leaves the previous root writable. Stale or cross-writer commit
acknowledgements are rejected.

The footer indexes root sections, node batches, and derived statistics. Native
and WASM readers use the same Rust implementation. Ordinary reads do not repeat
CRC checks or decode other versions' node payloads. Explicit `recover(bytes)`
rebuilds the directory from intact root and node frames if the footer was lost;
statistics in a lost footer must be recomputed. Complete corrupt frames fail
recovery rather than silently losing data. State replication remains separate
from `.rx` retrieval and preserves the immutable node prefix.

The Commit Scheduler limits concurrent hourly packing and canonical writes
across stores, with one worker by default. Publication retains the existing
writer-lock ordering. Pack sealing remains one step within the hourly work.

The registered leaf publication step now uses this scheduler, a per-type writer
lane, and the existing immutable `/leaves` snapshots. Its integration tests cover
write failure/retry, scheduler blocking, version handoff, and reopening. The
automatic hourly producer still uses the old Store's preparation path; connecting
that producer to Rust commit preparation remains required before the cutover is
complete.

Branch time tries use positional UTC year/day/hour navigation. Branch indexes
carry fixed event rows; collection and Tantivy references identify the source,
hour, and record. Keep object identity separate from event identity. `level` and
`access` are independent query fields. A completed request is not automatically
a committed object mutation.

Configuration inherits through root `trunk.conf`, `<cache>.conf`,
`<service>.conf`, `<resource>.conf`, and `<branch>.conf`. Each named config sits
inside the directory it configures. Only `trunk.conf` is created by default;
child files appear when explicitly configured. The cache file combines `defaults`,
`resources`, `policy`, and `inherit` sections without changing precedence. Per-resource/branch policy controls
retention and compression. Source implementations live in
[`go/server/cache`](go/server/cache), [`rust/src`](rust/src), and
[`../snoot/client/services`](../snoot/client/services).

Snoot Cache Quotas inventories registered physical storage as Trunk → Cache →
Service → Resource → Branch. Resource minors are combined in the view. Branches
come from opened cataloged stores, not combinations of `level` and `access`
fields. File allocation is measured on disk; shared metadata stays with its
owning parent. Hourly recovery estimates use segment-size weights. Quota settings
are saved on the server, but automatic space recovery is not connected yet. The
allocation diagram uses a fixed height and one scale across all columns; zero
allocations remain zero and are still accessible through Select Node.

Trunks also exposes Compression, Staging, Indexing & Replay, and
Replication editors. These read the same inheritance chain as ingestion and
save explicit overrides into the scope `.conf` files. Saves validate
values, preserve unrelated settings, and reject concurrent edits using file
revisions. Restart the server to activate saved changes. Transforms currently
provides read-only inspection of each resource definition’s `transforms.cfg`.
Summary UTC Offset (`summary_utc_offset_hours`) follows the same Trunk → Cache →
Service → Resource → Branch inheritance. Whole-hour offsets from −12 through +14
keep summary days exactly 24 hours; an explicit zero overrides a parent offset,
and removing the override restores inheritance. The default is UTC. User profile
time zones affect presentation only. Applying an offset repackages summary days
and calendar rollups without rewriting event packs or indexes.

Quota settings live in the same scoped files under `[quota]` (`limit_bytes`,
optional `recovery`). Snoot assembles their explicit overrides into the allocation
view; automatic inherited shares are never saved. There is no separate quota
file. Quota and policy editors preserve each other’s sections. Each changed file
is atomically replaced; a multi-scope save is not a crash-atomic transaction
across files. On a save error, reload to inspect the saved scopes.

## Build and Run

Requires Node 22.12+, Go 1.24+, CGO with a C linker, and Rust 1.89+ including
`wasm32-unknown-unknown`. Build Rust before the Go executable.

```sh
rustup target add wasm32-unknown-unknown
make build
./trunk ingest 5410 serve 8443 --http-address 127.0.0.1:3020
```

Runtime configuration, credentials, certificates, and caches are local and
ignored. See [`.env.example`](.env.example) and the CLI implementation under
[`go/cmd/trunk`](go/cmd/trunk) for configuration. Build the database with `make build`. Snoot has its own release and web
listener. For the sandbox, with the two checkouts beside each other:

```sh
make -C ../snoot build
make -C ../snoot serve           # http://127.0.0.1:3021, API on 3020
make site                        # public website
```

The Go frontend serves Snoot assets and can forward database traffic to the
configured Trunk origin. Trunk retains ownership of the datastore and all API
operations. Existing deployments can explicitly serve a static directory with
`trunk serve --web <directory>`.

### Public Website

The separate Cloudflare Pages project is `trietrunk`. Its homepage contains
Trunk, Snoot, Seeds, and Bench sections. Bench has Performance, Storage, and
Transit tabs. Performance currently displays explicitly labelled sample values;
Storage and Transit use the measured million-event fixture.
`/PIL` holds the format, library examples, and Definitions notebook. The earlier
detail pages remain at `/hidden`. After committing website changes, publish with:

```sh
node site/publish.mjs
```

Publishing rebuilds all routes together from the current source and refuses
uncommitted website changes. Do not deploy an older temporary artifact to update
one page. `/release.json` records the commit and homepage/PIL hashes so the
combined release can be verified. `node site/build.mjs` remains available for
local builds; `BENCH_PREVIEW=1` writes a noindex local preview to
`site/artifacts/bench-preview`, separate from the production output.

The website uses checked benchmark JSON at build time and generates its
measurement download without depending on local Markdown reports. Shared artwork
uses the front-facing Snoot anteater for all logos and lockups; the side profile
is reserved for profile/avatar icons. The trunk-sniffing composite is not used.
Snoot offers Light, Dark, and Auto themes; saved preferences persist.

Snoot duration comparisons default to milliseconds: `events | where duration <= 10000`
means at most ten seconds. Explicit `ns`, `us` (or `µs`), `ms`, and `s` suffixes
override that default. The Definition’s duration role identifies the field;
storage retains integer microseconds. Indexed and residual comparisons normalize
the operand exactly, without rounding stored values. Search Guide and field
completions expose the query unit. Refresh preserves the selected time window
independently of disposable scroll geometry.

Definition-declared object IDs have a distinct link color. Clicking one opens
Object History and performs a fresh `/<cache>/<service>/<resource>/collections?id=<id>` lookup in its resource.
Debug and trace leaf buttons sit in the event margin and open separate data
dialogs. They use `correlation_id` or `trace_id` when supplied, with an exact
millisecond timestamp fallback within the same resource. Their records are
excluded from normal event counts, histograms, distributions, and operation
rankings. Diagnostic dialogs request pages without event summaries.

Native collection rejects conflicting physical generations before publishing
coverage. A recreated or restored server dataset can have the same Definition
and generation sequence as an older local replica but a different generation
ID; that is an identity conflict, not evidence of damaged event bytes. Temporary
publication races retry once with fresh coverage. Persistent conflicts keep the
existing replica intact and expose **Refresh Local Cache** beside the error.
Search gathers conflicts across the page instead of stopping at the first
failed resource. That action resets all identified resource caches in one
durable transaction, fences in-flight writes, and reruns the search. If a
refreshed resource still conflicts, Conflict Details replaces the refresh loop. Other resources, dashboard preferences and server
data are retained. Worker errors preserve generation IDs, sequence numbers and
state epochs for diagnosis in the console; corruption/checksum errors are not
silently converted into retryable conflicts.


Native event storage uses OPFS under `snoot-store/<credential hash>/`. Each
branch holds its sparse `events.beds`, `events.indx`, state trie/definition
files, and raw time trie. Committed time-trie leaves determine availability;
there is no second per-event IndexedDB index or persisted package-coverage list.
Allocation/decoding context is stored in `layout.<revision>.pil`. The atomic
`commit.pil` root publishes immutable layout and `branch.<revision>.time`
snapshots after data files have been flushed. Native responses queue up to 4 MiB
or 4,096 control records per batch (a single larger PAC is flushed immediately).
Small responses publish once after wire completion, rather than committing
metadata for each decoded record. Each BEDS hour is written with its header and
payload together (split only at physical sparse-page boundaries). Open file
handles, sizes, and retired-byte headers are reused within the batch; dirty
files are flushed before the time-trie snapshot is published. File metadata is reused within one origin
lease and reloaded at the next command, keeping cross-tab visibility intact.
Sealed search hours already present in the committed trie are read locally
without another collection request. Failed publication leaves the
previous committed leaves authoritative. Saved collection bookmarks use a
separate OPFS file, not IndexedDB. Summary PACs are expanded before entering
OPFS or the bounded Cache Storage cache.

Client completeness concerns the files being synchronized. A beds completion bit
is published after its data is committed, while other replicated files can become
available independently. Default Search collects whole hourly event packs before
evaluating predicates locally. Client file-completion booleans belong in a compact mask;
the server does not maintain a client receipt mask.

Time trie artifacts use `.time`; taxonomy and state tries use `.trie`. The
64-byte time header stores the shared hour origin at bytes 16–23. Hour
references resolve against this origin, not the retained first hour.

Server time tries contain sealed hourly address/count leaves, without a status
tail. Sparse client copies append one file-completion byte: summary `1`, index
`2`, state `4`, inverted `8`, and beds `16`. Zero means no associated files are
complete. Sparse allocation ranges identify received time leaves, including
known empty hours, independently of those file bits. Importing time alone does
not certify any other file. A time refresh preserves committed file bits. Header byte 47 carries the signed
summary UTC offset in hours. When that offset changes, only summary completeness
is cleared, including cached hours outside the refreshed range.

The connection discovers registered sources through `/caches` (PIL), then reads
`/<cache>/registries?kind=inventory`. This catalog lists actual registered files;
it does not embed Definitions or trie bytes. Definitions and native state/taxonomy
files are retrieved separately, validated, and committed before publishing the
local discovery snapshot. The default registry body is the selected cache’s
PIL registration record from the `trunk.trie` footer;
`kind=taxonomy` selects the trunk-wide `trunk.trie` bytes. Discovery is independent
of query execution.

`trunk.trie` is the authoritative taxonomy, replicated in full even when the
selected query scope is narrower. The taxonomy response carries an ETag;
conditional requests reuse the saved file on `304 Not Modified`. The existing
inventory remains an artifact-transfer manifest, not a second taxonomy tree.

`rust/trunk` exposes the same reader natively and as `../snoot/client/assets/trunk.wasm`.
`Trunk::open(bytes)` retains an immutable published image. `node`, `children`,
`path`, `find`, and `branch` traverse that image; `section` borrows registration
payload bytes without interpreting their PIL encoding. Labels borrow the file.
The reader caches address indexes, with direct branch-ID lookup and hashed edge
lookup. These indexes are rebuilt once per opened image, not per traversal.
Snoot's source breadcrumbs use this reader rather than splitting catalog strings.

`resolve(scope_node, qualifiers)` returns stable branch IDs for an explicit
`cache.service.resource.type` source; omitted ancestors come from the selected
node. It never infers a resource type. The Rust SQL resolver borrows this API;
end-to-end qualified SQL routing remains separate from the taxonomy reader.
The Seed API exposes `trunk.trie().branches().select(...).time(bytes)` and
`.leaves().read(bytes)` using the selected registered version. Time addressing is
unchanged; leaf lookup selects its version root from the shared state file.

The Definition shape DAG is the accepted registration architecture and remains
part of the design unless explicitly replaced. The Rust `registration` feature
implements implicit Definition relationships.
`Trunk::registration(target, &document)` returns a new snapshot with the resource
binding and a structural DAG in footer section 2, encoded by Pill Packer. It
infers multiple bases and roots from named field shapes without author-declared
inheritance. Equal shapes share a node; strict containment forms the edges.
`Trunk::registration_with_binding` stores resource settings alongside that
registration: strategy, timezone, indexed paths, and reference translations.
The `.rx` remains the reusable DOC Definition; the same bytes can serve multiple
resources with independent settings. Binding updates preserve Definition hashes
and taxonomy addresses. Field annotations remain part of the Definition.

Namespaces optionally organize reusable Definitions. A Definition named
`transaction` can be registered under the `payment` service. Its ID annotation
`@object` identifies it as a business object. The service supplies the SQL
schema. Neither the service name nor the storage strategy is a required prefix
on the Definition. A namespaced Definition such as `log.http` can be reused by
several resources. Namespaces do not determine structural inheritance.

**Proposed Definition organization.** Publish shared bases such as `event` and
`log` at the trunk for use across services. Publish an application model such as
`transaction` at its owning resource, for example `prod/payment/transactions`.
Treat publication scope as registry metadata. Reuse a shared Definition through
a reference when binding a concrete type to a resource. Only a resource
binding creates a query source. Pin the exact registered revision on promotion.
The current registration API requires resource placement, and lineage lookup
still uses namespace and name across the trunk. Supporting resource-local names
requires scoped lineage and RX artifact resolution before exposing this workflow.

**PostgreSQL clients.** On an existing node, set `TRUNK_PG_PASSWORD` and run
`./trunk serve --dir <node-directory> --postgres 127.0.0.1:55432`.
In Beekeeper Studio select PostgreSQL, user `trunk`, the cache as the database,
and SSL disabled on loopback. The adapter exposes service as schema. Each bound
event type appears as one quoted relation combining its resource and registered
type, with namespace dots in the type rendered as dashes. For example:

```sql
SELECT * FROM customer."accounts.log-http" LIMIT 50;
```

This interface uses the node for event reads, whole-result and grouped aggregates,
and supported inner and left joins. Catalog queries operate on a read-only projection of the registry.
PODS queries and editing through the IDE are outside this
first adapter.

Branch and resource type name the same partition. An optional split uses
an explicitly configured finite grouping of a field. Registration accepts
`field.split` and `split.<name>` in its resource binding; each named split maps
a stable storage label to an array of scalar values or a comma-separated list. The Rust registration layer
checks that values fit the field and that groups do not overlap. Enum fields also require explicit group configuration. A single `*` is the
fallback for unmatched, missing, or null values; explicit values take precedence.
Without a wildcard, an unmatched event is rejected for the lost-event path. These settings are stored in
`trunk.trie`, independently of the reusable Definition and its digest.

For example:

```ini
field.split=level
split.trace=1,2,3,4
split.debug=5,6,7,8
split.info=9,10,11,12
split.error=13,14,15,16,17,18,19,20,21,22,23,24
```

The authored log example stores `level` as `uint(1)`. Presentation metadata
groups 1–4 as TRACE, 5–8 as DEBUG, 9–12 as INFO, 13–16 as WARN, 17–20 as ERROR,
and 21–24 as FATAL. The HTTP example independently registers trace (1–4),
debug (5–8), info (9–12), and error (13–24) partitions. These assignments do not
replace the numeric field with an enum or discard its original rank.
The routing plan returns unmatched events to its caller; writing `lost.packs`
belongs to the ingestion integration.

A Definition PAC can hold one version, several versions, and composition
dependencies. Member 0 is the default prescription. DOC's
`prescribe_version(namespace, name, major, minor)` selects an exact version;
`prescribe_member(ordinal)` selects a member without moving object pointers.
The Seed WASM wrapper accepts
`seeds.prescribe(bytes, {namespace: "log", name: "http", major: 0, minor: 1})`;
omitting the selection uses member 0. Rust resolves the selection and compiles
the prescription once; the host wrapper does not parse Definition bytes.
Registration accepts an optional `Selection` with the same identity/version.
Branches retain exact Definition references, independently of the bundle's
current default. `Registry::prescribe` resolves that registered identity and
checks its stored digest, so moving a member within a lineage bundle cannot
silently change its interpretation. This comparison does not recompute hashes.

`Registry::lineage` lists versions of one exact name/namespace.
`resources_at(trunk, node)` discovers registered branches below a taxonomy node,
without inferring bindings for unregistered branches. The WASM facade exposes
these as `lineage(name, namespace)` and `registrations(node)`. A published bundle
can grow or change its default while retaining its previous schemas; replacement
cannot remove them or change a published version's schema. Annotation-only edits
remain supported. Separate `.rx` files for individual versions are also valid.

The native `RegisterSeedDefinition` adapter installs the authored `.rx` and
publishes the Rust-produced trie through the existing registration lease and
durable footer transaction. The WASM reader exposes the same bindings through
`definitionGraph().resources`. Native publication, restart, and WASM readback
are tested. The live HTTP registration endpoint and production query planning
have not yet switched to this path.

In Snoot, **Models** in the main drawer opens data definition design. Its Model,
Definitions, and Fields views display the registry through the Rust trunk reader,
with multiple-base relationships, Definition details, resource bindings, search,
and zoom. The client computes display coordinates only; it does not infer a
second schema graph. A trunk without shape registrations displays an empty state.
Live registration, sample ingestion, and design assistance are subsequent
workflows within this view, not actions currently exposed by it.

Field order does not affect relationships, including fields inside inline and
referenced objects. Concrete Definitions retain their original field order and
digest; ordinal mappings belong to access planning for that specific Definition.
Rebalancing a layout therefore preserves its DAG relationships. Structural
matching compares declared types and sizes without interpreting application
annotations or inferring units. Nested values still use their concrete Definition
when decoded; a shared shape does not imply an interchangeable byte layout.

The builder example authors seven `.rx` prescriptions and registers their shapes
in reverse dependency order, including a Definition with two independent bases:

```sh
cargo run --manifest-path rust/trunk/Cargo.toml --features registration \
  --example author_shapes -- rust/trunk/examples/implicit-shapes
```

The footer’s keyed extension sections precede the name directory. Their section
IDs and relative bounds are independent of node addresses and name references.
Section 1 contains cache registrations, encoded with the existing PIL codec;
unknown sections survive publication. The final 16-byte name-directory anchor
stays fixed in form as new sections are introduced. Footer-only updates preserve
all node bytes. Registration uses a trunk-wide publication lease, never a search
or ingress lease. Interrupted writes recover a complete old or new footer.

`make build` copies the already-built Pill Packer `doc` and `rx` beside `trunk`.
Registration through `trunk definitions register` or HTTP accepts the compiled
RX PIL document directly. The shared Rust node prescribes it without invoking
external DOC/RX executables. The old prototype tool installer is excluded from
normal CLI builds.

Taxonomy nodes are fixed-width 20-byte records addressed by node ordinal. Only
terminal branches receive a 16-bit Branch ID (1–65,535); intermediate nodes do
not consume this namespace. Names, types, and levels are interned in an
EOF-relative directory. New labels prepend to that directory so existing
references remain stable. Registration appends nodes and re-tails the directory;
unchanged registrations perform no taxonomy write. Two checked commit slots in
the fixed header let recovery select a complete registration. Before overwriting
the old directory, registration durably publishes a same-file recovery copy;
existing node bytes are never rewritten. The final commit permits truncating
that temporary tail. Search reads use an immutable published snapshot and do not
participate in registration writes. The state trie retains its existing shared
Definition scope and 16-bit node references.

Registration requires explicit service, resource and type placement through
`Resource-Service`, `Resource-Name`, and `Resource-Type` HTTP headers, or their
CLI parameters. Namespace/name/version identity comes from the RX document.
Updates use the same placement and preserve the registered configuration;
`Resource-Split` selects a configured split when applicable. `Trunk-Branch`
remains an optional internal selector, not a required public update identifier.

The Seed identity contract uses `id: bytes(10)` annotated `@event` or `@log`, with no
rate-dependent width. Field sizes belong to the Definition; timestamp units
belong to the Seed annotation. Bare `@time` means Unix milliseconds and uses
74 random bits for UUIDv7 rendering. The `@time(us)` and `@time(ns)`
profiles reserve 12 bits of that UUID payload for fractional milliseconds,
leaving 62 random bits; ID storage remains ten bytes. The fraction is derived
from the full timestamp, not stored a second time in the identity. UUIDv7's
fraction cannot preserve every nanosecond value, so the original timestamp
remains authoritative. These annotations are interpreted by Seeds, not PIL.

Rust Seed result views and materialized objects reconstruct the full 16-byte
UUIDv7 when accessing the annotated ID. The annotation binding is prepared once
per Definition; the timestamp may come from the Pill or its storage index.
Projection of the ID alone still resolves its timestamp. Stored Pills retain
the ten-byte ID and separate time unchanged. Snoot uses the base annotation to render those
returned bytes in the standard hyphenated UUID format. Missing and null IDs
remain missing and null; an invalid timestamp cannot fabricate a UUID.

The Rust-authored event Definitions use the fixed ID width. The active older
ingress path still uses the previous five-to-ten-byte rate-dependent generator;
its replacement and the sub-millisecond profiles are pending Seed integration.
Six-byte millisecond timestamps also require unsigned six-byte execution support
in RX; the current authored event uses eight-byte time until that is implemented.

The ingress meter retains only current millisecond/hour counters and their
peaks, shared across the trunk's caches and branches. It counts accepted new
ingress, not recovery or sealing, and resets on process restart. Hour buckets
start at meter creation. Atomic counters allocate no per-event memory; generated
identity bytes reuse writer scratch before encoding into the existing native
buffer. Every branch `/summaries` response adds the optional `Trunk-Ingress`
header: `since=<epoch_ms>, peak-ms=<count>, peak-hour=<count>, entropy-bytes=<n>`.
The resource body remains unchanged. Snoot shows received peaks in Overview;
these are trunk-wide arrival statistics, independent of the selected query.

Resource identity belongs in the path. Accessors belong in query parameters:

| Endpoint | Data |
| --- | --- |
| `/<cache>/definitions` | Raw `.rx` by `definition=<digest>`; Definition registration |
| `/<cache>/registries` | Registry document, inventory, or taxonomy, selected by `kind` |
| `/<cache>/<service>/<resource>/leaves` | Type/major-scoped `<type>.v<major>.leaf`, selected by `Trunk-Branch`, with per-version roots |
| `/<cache>/<service>/<resource>/collections` | Existing object history and subscription-state operations |
| `/<cache>/<service>/<resource>/<branch>/packs` | Hourly seed PACs and the available unsealed interval |
| `/<cache>/<service>/<resource>/<branch>/strings` | Selected BOX common strings for sealed PACs |
| `/<cache>/<service>/<resource>/<branch>/summaries` | Whole daily summary PACs |
| `/<cache>/<service>/<resource>/<branch>/timelines` | Native header and selected hourly time-trie leaves |
| `/<cache>/<service>/<resource>/<branch>/indexes` | Complete native hourly index segments |

`resource` has no version suffix. `Trunk-Branch` selects an exact registered
Definition and therefore its physical major generation; `definition=<digest>`
checks that binding. Filenames never become separate resource URLs.
Each replication stream selects one branch. `beginTime` is inclusive and
`endTime` exclusive, in epoch milliseconds. Omit `endTime` on seeds for a finite
snapshot through the server's captured current time, rather than a continuous tail.

Each origin tracks frequent variable text per BOX in memory. The first
acknowledged event hour starts a 30-day observation window. During that window
new PACs use ordinary encoding. At each 30-day boundary the writer ranks its
bounded counters and shortlists the upper half (rounding up). Candidates need
at least three observations and recurrence in two distinct published hours.
The default capacity is 1,024 candidates (Rust `IngressLimits.string_candidates`).
Each retained string is at most 4 KiB. Misra–Gries counters are conservative
frequency estimates when distinct values exceed capacity. Restarting the owner
starts a fresh observation window. Published ordinals remain usable.

When a subsequent PAC contains shortlisted strings, the encoder compares its
actual stored bytes, including CRC, compression policy, and dictionary growth,
with ordinary encoding and, when applicable, reuse of existing ordinals alone.
A tie retains the smaller dictionary. Qualification and the byte comparison run
on newly sealing PACs. Previously published PAC bytes and addresses stay stable.

A sparse replica reads the BOX string ordinals referenced by each collected PAC.
For missing ordinals it requests `GET /<cache>/<service>/<resource>/<branch>/strings?id=2&id=7`
with the same `Trunk-Branch` and `Trunk-Generation` as the PAC response. The response
contains ordered JSON pairs such as `[[2,"pump started"],[7,"pump stopped"]]`.
One request accepts 1–256 distinct ordinals. An unknown ordinal returns 404 and
a generation mismatch returns 412. Rust verifies the PAC dictionary digest before
making records readable. The replica commits new entries with the PAC's Time coverage
and reuses them on subsequent collections.

Timelines accept repeated `range=begin_ms,end_ms` parameters for sparse whole-hour
ranges (or one `beginTime`/`endTime` pair). The response contains the native 64-byte
file header followed by those native leaves, without additional body coordinates.
The client derives placement from the requested ranges and published file bounds.
Indexes use `generation=<id>&rows=start,end` with repeated row ranges, including
the six-byte hour header. Responses concatenate the exact selected file segments.
Partial-hour index ranges are rejected. At most 128 sparse ranges share a request.
State snapshots preserve the original header and complete native blocks.

The temporary Tantivy diagnostic (`indexes?mode=search`) and collection history
are query operations, not raw index-file replication. The comparison currently requires an exact branch. Their result protocols
remain separate from these replication readers. No CDN caching policy is enabled.

Each seed request selects one exact branch and Definition. Repeated
`range=begin_ms,end_ms` parameters select sparse, complete UTC-hour intervals;
up to 128 ranges and 65,536 selected hours are accepted. They may accompany
`beginTime` with no `endTime` for an incremental live tail. The response is one
uncompressed PAC of PACs: unchanged stored hourly packs, followed by uncompressed
live-hour packs. No JSON envelope or stream-control records surround these bytes.
The outer named `seed.window` header carries the generation, generation sequence,
sealed boundary, retention boundary, and captured upper timestamp. Live children
carry `seed.hour` and `seed.rows`; only durable children carry native placement.

The shared worker issues independent branch requests concurrently through the
same configured origin. The browser controls HTTP/3 negotiation and connection
reuse. Timeline discovery precedes the coverage plan. That plan starts missing seed
and index streams independently; an unchanged live-only refresh requests only
its open seed tail. A seal arriving after discovery is usable immediately;
a post-result timeline refresh reconciles its canonical addresses while preserving
the local completion bits.
The seed reader applies backpressure and commits bounded batches of whole hours.
It reuses committed indexes rather than rewriting them, bridges known empty hours,
and publishes payload bits only after file flush. Live packs remain query-LRU
memory and never enter `.beds`.

Server payload memory has two separate structures per branch: immutable sealed
PACs for the latest 24 hours, and immutable ingress PILs awaiting sealing. An
acknowledged ingress batch publishes a new descriptor tail without changing
previously published PIL bytes. Sealing builds a separate PAC and appends complete
blocks to the datastore/index tails. After that commit succeeds, one atomic
publication replaces the hour's ingress references with the sealed PAC reference.
Maintenance publishes timeline/summary changes and evicts hour 25. Retained
readers keep prior buffers, mappings, and file handles valid until release.

Established resource reads acquire a retained publication without the endpoint
or store writer mutex. Registration publishes a replacement routing directory;
first creation of an unused branch remains a writer operation. Resource GETs do
not initiate sealing. Restart warms the resident PAC window. Historical seed
ranges read the durable file through a retained handle. Sealed events are no
longer retained as duplicate expanded PIL payloads; object-history reads
reconstruct those PILs from the PAC when needed.

Received complete blocks are readable in shared-worker memory while bounded
publication batches run independently. Sparse writes to different branches
do not hold up resident reads or independent network streams. The file worker
coordinates durable batches and commit publication; queries consume received bytes
without joining that publication queue. Open-ended searches batch coverage through the
current hour rather than reusing the displayed end timestamp.
Durable completion bits change only after
file-worker acknowledgement. Existing disk indexes can be loaded once after a
worker restart; newly received packs are not written and read back to serve a
query. Resident block bytes have a 128 MiB eviction target per context. A 32 MiB
pending-write threshold starts background draining; it does not pause retrieval.
Unpublished bytes remain resident even above those targets until writes succeed;
failed writes remain visible and retryable. Eviction requires successful publication. Ephemeral live records remain tied to SearchesLRU. Their resident window retains
the same common checkpoint, so a repeat search need not await the history
transaction; losing that window forces a full retrieval rather than using a
persisted checkpoint without its data.

The query worker sends
`view-page`, `view-summary`, and `view-dataset` messages to the requesting tab; `executeSearch()` invokes
its page callback, and `PagedResults` batches Solid's event/result signals.
Storage writes alone do not trigger a render or a global cross-tab broadcast.

Margin charts use dataset/timeframe aggregates from summary PACs, independent of
event predicates. Calendar-prepared whole-day ranges drive batched local reads
or sparse remote requests. Summary delivery and event results are independent:
neither waits for the other. Matched counts remain part of the event result.


Connection metadata is owned by the SharedWorker and isolated by server and
credentials. Page refresh reuses that snapshot immediately; a restarted worker
restores its committed local snapshot. Background conditional requests validate
the cache directory and registries, retrieving only missing or changed artifacts.
Neither validation nor metadata publication delays readiness from an existing
snapshot. Definitions remain addressable by digest alongside newer Definitions;
updating current bindings does not replace Definitions used by older events.

Time-window replication is owned by the calendar. Selecting a scope
or applying a window refreshes the selected server timeline ranges and reads local completion
through branch-scoped `/timelines` requests, with at most 32 concurrent streams.
The histogram shows sealed hourly counts before Search is invoked; debug/trace
branches do not contribute to ordinary event counts. Long ranges combine hourly
counts into at most 300 rendered bars. Time imports preserve existing completion
bits and publish no event/index completeness. Changing the calendar window no
longer submits an event search automatically. Reload restores the query editor
and selected window without executing the saved query. Typing, Clear, histogram
window selection, and navigation do not submit searches. A submitted query keeps
its prepared calendar binding across background calendar refreshes. Search/Enter
submit explicitly; result Refresh, Retry, and pagination remain explicit actions.

### Client Search Execution

Search uses catalog identities and hourly completeness bits to collect only missing
whole `.packs` packs. It does not call `/events`; index ranges use the independent `/indexes` stream. The first nonempty hour is scheduled separately for
prompt first-page results; remaining work is batched across the requested range. Independent branch
network streams use up to 32 tasks; CPU/page work retains its separate bound. Each window is matched locally after collection.
The server determines whether each returned PAC is sealed. A PAC that sealed
after calendar refresh supplies its own hour and index addresses; it is readable
without another timeline request. Its completed local receipt is reconciled with
the canonical time-trie leaf by a background `/timelines` task after query results
are delivered. The task refreshes affected head hours per branch and preserves
existing completion bits; searches reuse the PAC while reconciliation runs.
Failed refreshes remain eligible for retry. Unsealed PACs stay
ephemeral under SearchesLRU and never enter `.beds`. The default Search path
never asks the server to evaluate predicates.

State leaves reject impossible matches before decoding event values. Compiled
predicates support AND, OR, XOR, NOT, and parentheses; precedence is NOT, AND,
XOR, OR. An OR match is counted once even when both conditions are true.
Text matching currently searches Definition enum values; text absent from those
Definitions is ignored and reported. Free-form event text and production client
Tantivy search remain deferred. The Tantivy Comparison option invokes the server
explicitly and rejects Boolean expressions it cannot represent.

Native paging retains the two binary boundary searches. Independent aligned
read windows serve index probes, sequential rows, PAC directories, and payloads.
Small first-page reads grow to bounded 4,096-record scans, subject to the existing
byte cap. Complete query aggregates are cached for at most eight exact snapshots;
later pages seek to their cursor timestamp, resolve ties by the full source key,
and stop after the requested page. A changed query, window, generation, or local
rebase invalidates reuse. A fresh worker can rebuild aggregates from durable data
without recollecting complete hours. Local latency rankings report their actual
average, rather than labeling that mean as P95.

Definition-bound five-minute accumulators update at ingress on every branch.
Numeric fields retain count, null count, min/max, and sum; integer sums use 128
bits and floating sums use compensated addition. Timestamp fields retain bounds,
boolean fields retain counts and bounds, and other fields retain presence/null
counts. Non-finite inputs suppress unsafe proofs. Sealing-assigned record/state
identities are excluded from ingress bounds. Required fields and optional fields
use the same rules, with absent and null kept distinct.

Two hour slots cover normal ingress and the previous hour's grace period.
Successful sealing alone releases a slot; backlog lives in overflow slots until
sealed. Published snapshots are immutable. Restart recovery may rebuild cells
from saved ingress. Summary requests encode these cells without scanning events.
Generic hourly PACs contain twelve positional five-minute records, including
empty records, plus ordinal/type and full-hour rollup headers. Aggregate cells
use PIL arrays to support the entire Definition field limit. These PACs travel
inside existing daily summary data and ephemeral live-summary headers.

Live checkpoints identify a completed five-minute prefix. Late arrivals invalidate
changed prefixes. Live summary data expires with Searches LRU ownership.
Typed bounds can exclude a whole hour or a selected portion, including `now()`.
Missing bounds preserve retrieval; possible live matches keep the open-ended
seed request. Exclusion never marks event data replicated.

Summary PACs default to Zstd level 1 during daily sealing and travel in that
compressed form. The client expands them before storage in OPFS `branch.summ`
or Cache Storage, preserving daily rollup headers and hourly PIL members.
Search and service dashboards share the expanded PACs without decompressing on
each read. Origin identities and local-byte integrity are tracked separately.
Their bits publish only after durable storage and agreement with received hourly
counts. Search event charts aggregate the locally matched events; Fast dashboards
consume precomputed summary columns. The performance flyover separates planning,
collection, and client work.

`go test ./server/cache -run TestSearchPlannerWorkerAgainstHTTP -v` exercises the
production worker and WASM codec against an isolated Go server: cold/warm search,
restart, paging, ignored text, truncated whole-pack delivery, and failed summary
publication followed by retry. It asserts that default Search never requests
`/events` or `/collect`; indexes replicate independently through their branch resource. Client tests also cover Boolean combinations,
same-timestamp cross-branch paging, cache invalidation, and cancellation. The
Node process is a test client, not an application server. Browser verification
covers the actual Search page and real storage-worker/OPFS I/O separately.

### Client Search Measurements

A development stress test on 1,048,576 synthetic events in 256 hourly packs used Chromium,
production WASM, storage-worker messaging, and durable OPFS. Identity and storage
formats were identical. Both runs returned the exact count and disjoint pages.
The before build was reconstructed from development sources saved before the
optimization pass, not from the deployed release. The unfiltered query requested
100 events while aggregating all million records; the first page was ready long
before the full request finished.

| Measurement | Before | After |
| --- | ---: | ---: |
| Full local scan and statistics | 132.83 s | 5.36 s |
| First 100-event page available | 64.5 ms | 49.0 ms |
| Next 100-event page ready for UI | 62.3 ms | 42.7 ms |
| Subsequent request complete, including statistics | 132.08 s | 42.7 ms |
| Storage-worker page reads per full scan | 8,192 | 268 |

These are sequential single-run browser measurements, not production latency
claims or a deployed-versus-new comparison. Next-page completion improves mainly
by reusing statistics instead of repeating the full scan. Fixture transport was
cached; real network transfer and DOM rendering
are excluded. The full scan remains linear in the number of collected candidates;
this change does not turn event analytics into summary-column performance.
[Raw Chromium results](benchmarks/client/2026-10-29-planner/chromium.json).

A separate three-run memory-backed I/O harness measured median fresh full scans
at 19.26 s before and 1.91 s after. File read requests fell from 96.78 GB to
112.08 MB per scan; those counters measure requested bytes, not physical disk
traffic. [Before](benchmarks/client/2026-10-29-planner/memory-before.json) ·
[After](benchmarks/client/2026-10-29-planner/memory-after.json).

Reproduce the memory harness with
`node --experimental-strip-types ../snoot/client/scripts/benchmark-planner.mjs <checkout> <fixture.pil> <report.json>`.
The fixture requires its adjacent `.pil.json` metadata. Build the browser worker
with `node ../snoot/client/scripts/build-planner-benchmark.mjs`; serve its output, WASM,
and `/fixture/dense.pil` plus metadata through the Go test server, then start
`worker.js` as a module worker and post `{variant: "after"}`. It reports timings,
checks counts/page overlap, and removes its own temporary OPFS namespace.

Snoot's Profile page has Preferences and Connections tabs. Preferences include
name, Light/Dark/System appearance, Trunk/Snoot theme, and events per page.
Connection registrations are local profile data until identity-server integration.
Each enabled registration performs independent registry and Definition discovery at startup. The
selected server supplies the current workspace controls. Bearer tokens remain
separate per registration in tab memory; they are not saved with the profile.
Worker sessions and durable caches are isolated by server and credential.
Remote servers must permit browser requests from the app's origin.

Only complete collected hours enter durable event coverage; partial query results
remain in memory. A received time leaf establishes count/address availability;
its beds bit establishes committed event-data availability. Other bits certify validated, committed
contents for the matching source and generation. Sparse allocation mappings and
record counts serve address calculations, rather than file-completion tracking.

Storage names are `.packs` for the Pack container store and `.beds` for the
browsable event datastore.
Either storage role can run on a server or client node. Protocol
`MemberRanges` denotes decoded PAC member coordinates. The `replication.data`
policy and `data=none` collection option control data fetching. Recreate
version-zero caches after the serialized-name and file-identifier cleanup.

### Tantivy Reader Experiment

The read-only WASM reader in `rust/search-wasm` opens immutable Tantivy 0.25
files through synchronous worker file handles. It shares schema construction,
document indexing, text modes, and typed predicate execution with the native
server through `rust/search`. PIL/PAC encoding and compression remain in
pill-packer. The reader has a bounded 8 MiB cache of 64 KiB index blocks;
posting/column reads reuse those blocks instead of making one OPFS call per
small value. Original event payloads are not stored in Tantivy.

Time bounds come from the branch time trie and binary searches of its indexed millisecond
index. Tantivy evaluates text and indexed predicates, then its candidate
collector clips canonical event addresses to those exact snapshot ranges. It
does not enumerate address terms to construct a time-range bitset. Each accepted
address is resolved through the captured time trie before record retrieval.
Tantivy document IDs remain segment-local; they are not event addresses. Pure
time-window searches bypass Tantivy. Broad text matches still visit matching
postings outside the requested window; time-partitioned index storage is deferred.

This Tantivy reader is tested separately and is not invoked by Snoot Search.
Default matching runs in the client planner described above; only explicit
Tantivy comparison uses server search. Whole `.packs` transfers are decoded into
`.beds`, and file-completion bits publish after durable commit.

Replication uses these boundaries; the inverted-index stage remains deferred:

- State/filter parameters remain part of the request contract. Compressed PAC
  collection transfers whole requested hourly packs without scanning state
  references to decide whether an hour is worth transferring. A collection
  response does not claim that its records satisfy those predicates.
- Uncompressed `write.packs` still transfers whole packs. A future heavy-server
  mode may evaluate full searches over uncompressed data; it is not implemented
  by this replication policy.
- Inverted-index replication follows Tantivy's native segment/file layout, not
  forced hourly indexes. Transfer metadata and referenced files from one pinned
  committed snapshot. Existing immutable files may be reused; merges produce
  replacement segments. Keep the prior snapshot usable until the replacement
  is complete. The time trie records coverage of that snapshot; an hour's
  inverted bit requires all index files needed to search that hour, not merely
  one received segment. A snapshot may cover multiple hours.

Production `.tvy` snapshot pinning, publication, and planner integration remain
unimplemented. The default planner uses `.packs`; Fast dashboards use `.summ`.
Independent `.indx` replication exists but is not a prerequisite of default Search.
Source/Definition/generation binding, cancellation,
retention, and credential isolation apply to each artifact. Index and summary
bytes stay in OPFS, outside IndexedDB and session storage.

Build the reader with `node ../snoot/client/scripts/build-search-wasm.mjs` after installing
`wasm-bindgen-cli` 0.2.129; `WASM_BINDGEN` can point to an isolated installation.
This is build tooling, not an application runtime. Generate native parity
fixtures with `cargo run --release --locked --manifest-path rust/Cargo.toml
--example search-replication -- <new-output-directory> 256 2000`. Serve the generated
directory at `/fixture`, generated `../snoot/client/assets/search` at `/search`, and
`../snoot/client/scripts/test-search-replication-worker.mjs` as a module worker on a local
secure origin. Post one message to start; it writes verified files into a
temporary OPFS directory, compares browser results to native results, reports
timings, and removes its test files. Measurements in
`benchmarks/retrieval/2026-10-27-client-tantivy` cover 512,000 synthetic events
across 256 hourly indexes. They are reader parity/compute measurements, not
end-to-end application latency or product comparisons.

Dashboards has a collapsible Data Seeds directory and a Data Seeds dialog
backed by the authorized `/caches` catalog. Subscriptions target services within a cache and automatically cover their
current and future catalog resources. Resources are dashboard context, not
individual subscriptions. The `snoot-profile` IndexedDB database contains the
`preferences` store: theme, user-authored highlighting rules, and service
subscription choices. Selected service, drawer state, page, query, time window,
and other tab settings live in session storage under readable `snoot:` keys.
Search results, event bytes, definitions, and trie fragments are never copied
into session storage. A reload reruns the query using the local file cache.

The `snoot-searches` IndexedDB database holds bounded query metadata in
`SearchesLRU`: up to 20 predicate signatures, expiring two hours after their last
successful evaluation. Reads update recency without extending expiry. Signatures
exclude window, ordering, taxonomy selection, and connection identity; a separate
hashed context guards checkpoint reuse across sources, scopes, and Definitions.
The selected window is not persisted in the entry; an extensible properties map
allows additional query metadata.
Each entry can hold one newest-common timestamp (the minimum of each scanned
branch's newest timestamp), run counts, and measurements. Branch timestamps are
transient, and no event bytes or credentials are stored here. Checkpoints are
recorded only after full scans, before predicate rejection, with a one-millisecond
overlap for resumption. Searches ending at now use the ordinary branch-scoped `/packs` range:
its retrieval start advances to the checkpoint minus that overlap, while the
query's original window stays unchanged. The server captures `now` once. It sends
sealed PACs unchanged inside a PAC of PACs and unsealed records as uncompressed,
Definition-bound PACs with shared values, member addressing, and Seed time/index
headers. Predicates remain client-side.

Unsealed records append to a bounded query cache in worker memory (20 queries,
two hours, 64 MB combined); IndexedDB contains metadata only. Overlapping records
are deduplicated by provisional time/record identity. History eviction broadcasts
the predicate identity to workers, releasing its unsealed cache immediately;
in-flight work cannot republish an evicted snapshot. Newly sealed hours are
committed to local BEDS before their ephemeral copies are released, including
hours sealed behind the incremental start. Reload or eviction retrieves the full
window again rather than using a checkpoint without its cached records. An empty
branch contributes the evaluated upper bound to the common checkpoint.

A collection respects its requested lower bound; backdated ingress earlier than
that bound requires a wider retrieval window. The live response is bounded to
64 MB of source PIL records across selected branches and fails explicitly above
that limit. Historical packs retain batched native retrieval. Refreshing still
re-evaluates local predicates and aggregates; incremental transfer does not imply
incremental aggregate computation.
[Isolated WebKit measurements](benchmarks/retrieval/2026-10-30-window-collection/results.json) cover first retrieval, overlap-only refresh, and one new event.
Profile → Storage → Recent Searches clears individual entries or the whole cache.

Subscription preferences use a `dashboard-services:` key partitioned by a
SHA-256 digest of the trimmed connection token.
The token itself is not stored there; changing credentials selects a separate
workspace. Atomic read-modify-write transactions prevent lost cross-tab updates,
and BroadcastChannel refreshes other open dashboard views after commits.
Unavailable services retain their saved subscription until explicitly removed.
The subscription dialog moves services between Selected and Available lists with
add/remove buttons. Subscribe commits the changes atomically and starts real
summary collection. A workspace-level coordinator continues when switching menu
pages, resumes saved subscriptions on reload, and cancels removed services.
The shared worker collects UTC year-to-date windows newest first, one background
window and one daily packet at a time. Concurrent tabs share the same service
job; requests use `include=summary` and never collect event pages or seed packs.
Completed subscriptions refresh the current and previous UTC day every five
minutes. New catalog resources trigger another year-to-date pass. Older history
is fetched on demand through summary queries, not eagerly replicated.

The Fast dashboard uses one shared scope and time selection for all panels.
Its default layout is defined in
[`../snoot/client/services/fast-dashboard.json`](../snoot/client/services/fast-dashboard.json), with
TypeScript contracts in `../snoot/client/services/fast-dashboard.ts`. JSON is an advanced
representation, available under the collapsed **Default Chart Definition**;
ordinary use is through service, resource, and time selectors. No chart-editor,
JSON import, or layout persistence is implemented yet.

The first layout uses a 12-column desktop grid, two columns on narrower desktop
views, and one column on mobile. Panel order is document order; `span` is 3, 4,
6, or 12. The default contains four values, two timecharts, Top/Bottom 10 resource
bars, services with sparklines, observed levels, and a resource table with CSV
export. Boolean status widgets remain supported but are not in the default
layout. Bottom 10 includes active resources only.
Value widgets accept `view.indicator`: `trend` (Trend Arrow), `sparkline`
(Sparkline), or `none` (None, also the default when omitted). This is a display
choice for the same metric and selected window, ready for a future toolbox
selector. Sparklines show the bucket-by-bucket shape with a zero baseline and
gaps for missing samples; arrows summarize the direction and degree of change.
The four headline statistics use `view.indicator: "trend"` for an arrow comparing
the earlier and later halves of the selected window. Counts and interval aggregates are
combined in Rust; means are weighted and P95 Period Mean Duration uses the
distribution of five-minute means across the selected window. Tilt is 90 degrees times the difference
divided by the larger value: doubling tilts up 45 degrees, halving tilts down 45.
Changes below 1% remain horizontal. Missing comparison data shows a dash. Hover
text and accessible labels give both half-window values. This adds no requests
or event scans; service-list sparklines remain unchanged.
The **Deep** tab is disabled until event-based analysis is implemented.

### Chart Definition Contract

A dashboard has `id`, `title`, `mode`, `source`, `scope`, `time`, `layout`, and
`panels`. In the initial implementation, `mode` is `fast`, `source` is
`summaries`, and `scope` is `subscriptions`. The selected service and resource
narrow that subscribed scope at runtime. The default time selection is `30d`,
UTC, complete days; selectors also offer `7d` and year to date. Hours are used
for charts spanning up to seven days; larger ranges use days. This is display
aggregation over the underlying five-minute summary cells.

Each panel separates its result shape from its visual presentation:

| Result Shape | Representation | Compatible First Views |
| --- | --- | --- |
| `number` | One aggregate value; null means no samples | `value` |
| `boolean` | A typed comparison, initially `gt` against a number | `status` |
| `set` | Distinct observed dimension members with counts | `tags` |
| `table` | Grouped metric rows | `table`, `bars`, `list-trends` |
| `series` | Ordered time buckets with metric values | `timechart` |

A hash set is a possible execution structure for a `set`; JSON represents its
members as an array. CSV is an export of table results, not another query type.
Top/Bottom 10 are `order` plus `limit`, applied after aggregation. A trend is a
time series rendered compactly; a list with trends binds each group to its own
series. Missing duration samples remain null, not zero.

```json
{
  "id": "top",
  "title": "Top 10 Resources",
  "span": 6,
  "data": {
    "shape": "table",
    "metric": "events",
    "groupBy": "resource",
    "order": "desc",
    "limit": 10
  },
  "view": { "kind": "bars", "unit": "count" }
}
```

`metric` currently accepts `events`, `errors`, `errorRate`, `average`, and `p95`.
`groupBy` accepts `service`, `resource`, and `level`. `view.unit` is `count`,
`percent`, or `ms`; duration aggregates retain the duration role's microseconds
until display conversion. CSV headers explicitly identify microseconds and
percentages. A table's `view.columns` lists metrics; a boolean's `data.test`
contains `{ "op": "gt", "value": 0 }`. These are typed operations, not arbitrary
JavaScript or a chart-library configuration object.

For the future visual editor, the proposed flow is **Scope → Aggregate → Display**.
Dragging a toolbox item or choosing a compatible display creates this same JSON.
The result shape determines offered displays. Default layouts should stay
resettable; saved overrides and custom Fast layouts may rearrange or display
only summary-supported metrics. A request requiring missing dimensions or raw
fields belongs to Deep rather than silently falling back to event scans. Field
IDs, units, optional predicates, and additional reducers will be tied to the
Definition engine as that capability becomes available.

Fast performs packet decoding and histogram merging in the shared worker and
returns chart-sized aggregates. It reuses the same credential-partitioned summary
cache as subscription priming, obtains fresh manifests in seven-day windows,
and does not download event pages or seed packs. The first implementation uses
complete UTC days, up to one year; it is not a live-tail dashboard. It has no
independent polling loop: Refresh or a new completed subscription collection
reloads the selection. Unsubscribing or changing scope cancels the visible request.
Weighted duration means and nearest-rank p95 are calculated after merging
histogram weights, never by averaging averages or percentiles. Groups are ranked
only after combining all selected sources. Per-resource and service keys retain
cache identity to avoid collisions.

Some inventoried branches have event counts without precomputed summary columns.
The dashboard explicitly reports summarized events versus all matched events;
all displayed metrics and groups refer to the summarized data set. It does not
invent per-resource metrics for uncovered events or scan those events to fill
missing summaries. Tables export the same displayed data set. Summary dimension
incompleteness, duplicate sources, or overlapping requested scopes are errors.

Compressed sealed packets use a 256 MiB Cache Storage budget rather than a packet
count limit; decoded summaries retain their existing 32 MiB memory bound. Cache
pressure can evict older packets, so completing a pass is not a permanent offline
coverage guarantee. Unsealed packets remain in memory and are refreshed; priming
reports persistent-storage failures and offers Retry Collection. Fast charts use
these packets; Deep event analysis remains pending. Search has Events and Charts tabs sharing the same
query and time window. Switching tabs preserves loaded event pages without
restarting retrieval; the selected tab is restored with the search session.

## Snoot for macOS

The agreed native-node configuration belongs in **Trunks**. On first launch,
offer **Connect To An Existing Trunk** (address and credentials) and **Install Local Trunk**
(name and storage directory). Both become saved Trunk entries with an explicit
Local or Remote kind; a local node must not be represented by a blank remote
URL. The existing proxy-backed “Local Trunk” is not an embedded database.

For Install Local Trunk, the owner is native Rust: registration,
ingestion, recovery, commit scheduling, storage, planning, and query execution.
WebKit renders selected results and sends user actions. Local sealed Packs go
directly into uncompressed `.beds`, with no intermediate `.packs` or local HTTP
database hop. Open hours retain the existing memory and recovery lifecycle.
Windows share the native owner and writer for that Trunk. Remote entries retain
the existing connection and replication behavior.

UI enablement is a host setting, independent of Local/Remote ownership and
Pack/Browse storage. Snoot.app always includes and enables the full UI. Other
installations may serve it or run headless; ingestion, planning, summaries, and
queries must remain available through Rust without a UI. The existing server
selects its frontend with `serve --web <directory>` and disables it with
`serve --web=""` (or `TRUNK_WEB=`). The native Rust owner itself has no web assets
or UI dependency. Choosing Browse storage must never implicitly enable a UI.

First-launch choices, local directory initialization, exclusive native ownership,
bounded log inspection, reviewed JSON Lines imports, durable PIL ingestion,
live queries, hourly publication, and daily analytics use the bundled Trunk. `Snoot.app` bundles the public executable linked to the Rust node; no additional software is installed.
The app starts `trunk serve --dir <directory> --control-socket <private-socket> --stdio --env ""` and transports
requests to it. Rust loads the registered Definitions, selects Time/index ranges,
reads `.beds` through the existing positional reader, and retains result cursors.
WebKit receives only selected result pages; this local view starts no database
worker or WASM runtime. Native and WASM share query-request decoding.

The native socket test covers retained Definition generations and explicit
version restrictions. One logical resource remains one UI choice. Cursors share
a node-owned resident admission budget; at most eight are retained, with a 60-second
idle expiry checked on requests. Result pages are capped at 1,000 rows and 8 MiB;
fetches have a cooperative five-second cancellation deadline. These limits do
not represent a bound on total process RSS. Socket requests currently execute
serially; parallel scheduling has not been added.

The staged full UI now passes native SQL Search, zero-result Search, full UUIDv7
rendering and Models checks without a browser database worker or WASM. Summary
collection has a native adapter; missing summary coverage is reported rather
than displayed as zero. Full dashboard coverage is not certified by the small
event-only fixture. A separate first-launch WebKit test creates a fresh local
Trunk, reviews and imports unordered JSON events, then performs a filtered SQL
Search without an upstream. Native lifecycle tests cover live analytics, restart
recovery, and hourly/daily publication. The existing remote workspace continues
using its established worker/WASM path.
The integration
test `rust/seeds/tests/native_beds.rs` verifies the existing native storage
foundation: ingestion, plain Pack append, file reopen, and queries from one
`.beds` file. It does not certify the desktop ownership or onboarding cutover.
The remote-connection architecture below uses workers and WASM.

The universal distributable is `../snoot/desktop/dist/Snoot-macOS-universal.zip`;
`../snoot/desktop/dist/Snoot.app` contains both Apple Silicon and Intel executables.
The 2026-11-20 fresh-profile test ran on Apple Silicon with a system-only PATH,
no configured upstream, and no browser database worker. The build is ad-hoc
signed; Apple notarization is still a distribution task. Test and benchmark
evidence is in `research/benchmarks/standalone-2026-11-20/`.

One-shot `trunk beds query` and `trunk time query`
commands expose native reads independently of Snoot. They accept an idle local
directory or the running Rust node's private socket, emit tables to stdout
(TSV when piped, JSONL with `--json`), and
reserve stderr for errors and optional timing. See the
[CLI contracts and examples](go/cmd/trunk/README.md#one-shot-native-queries).

### First Launch And Branch Creation

The [onboarding workflow](research/plans/onboarding.md) records the staged review
steps, supported imports, and remaining format/mapping work. [External conversion and migration
plans](research/plans/data-transfer.md) are deferred; those connectors are not
part of the current onboarding implementation.

Source preparation belongs to `trunk.ports()`: format/standard detection, sizing
proposals, mappings, and integration adapters. Bounded inspection and sample
event-time review are implemented. Every resulting event requires UUIDv7 identity
and event time; the source needs only a timestamp, with Trie Trunk supplying
entropy when necessary. Existing valid UUIDv7 values are inspected first. See the
workflow for module boundaries, native integration guidance, and durability rules.

The deliverable is a self-contained `Snoot.app`: a recipient opens the app and
chooses connection or local installation. Local installation must provision and
start the bundled native Trunk without a terminal, separate server installation,
or an existing development socket. Returning users select saved Trunks. Native
ownership is an implementation detail of this experience, not a prerequisite the
user must arrange. Local initialization creates the taxonomy and data directory;
the bundled Rust owner handles registration, ingestion, recovery, publication,
queries, and analytics. Add Branch imports reviewed JSON Lines, CSV/TSV
and other delimited text, fixed columns, named line patterns, multiline logs,
logfmt, Syslog, and XML into a new branch. All formats use shared Rust Ports
and DOC authoring. Resumable connector jobs remain future work.

After connection or installation, **Models → Branches → Add Branch** can accept a log
file, inspect a bounded sample in Rust, and present a **Definition Candidate**
with parsed examples, inferred types/widths, timestamp/unit suggestions, and
unrecognized or conflicting values. Sampling cannot prove a whole file fits a
type; show its coverage and validate the full import against the approved
Definition. Never silently truncate, discard incompatible records, or register
a sample-derived schema without review.

Each type placement can have one mutable **Draft** (0.0). Saving a draft does
not add a published revision. Draft data uses `account.0.pods` for objects
and `log.security.0.packs` or `log.security.0.beds` for events. The node keeps
each disposable generation under `.trunk/drafts/`. Its token in `trunk.trie`
guards edits and promotion against stale clients. Production reads use only
published branches. In Models → Branches, **Save Draft** stages the Definition
and **Promote** publishes it. **Edit Draft** reopens the saved RX for field-name
and declared-size edits, including fields on referenced members. DOC applies
edits to their original ordinals. **Replace Draft** accepts a whole new
Definition. The first published version is 1.0.
Promotion permanently discards the selected draft's data, including its spools,
indexes and summaries, after publishing the Definition. Reopening the node
finishes reclamation if it was interrupted. The draft records never become
published records.

The CLI uses the same transaction. For example, save a draft with
`./trunk definitions draft --dir ./db --cache prod --service customer --resource accounts --type log.http --file log.http.rx`.
The JSON acknowledgement includes its generation. Supply that token to
`seeds test --generation TOKEN` with the same placement and a PIL input
file. Preview with `./trunk beds query --dir ./db --scope prod/customer/accounts/log.http --draft TOKEN --from START --to END`.
Then use `definitions promote --generation TOKEN` with the same placement
and node. Saving a changed schema over existing draft data requires
`--discard`. An ordinary query sees published data throughout.

The review selects the destination service/resource/branch and confirms field
mapping, time and identity handling. Then registration uses the existing
`trunk.trie()` APIs and file import uses the existing Seed ingestion,
accumulation and commit path. Parsing/inference belongs in reusable Rust tooling;
Snoot presents candidates, progress and actionable errors. Imports must be
streamed with bounded memory, cancellation and explicit error counts. Logger
integration is a later option, not an adoption requirement. Review Format shows
source/parsed examples, detection confidence, framing controls, and parse failures
before field review. Format profiles can be saved and loaded separately from RX.
Browser review and local macOS imports share limits of 4 MiB and 1,000 sampled
records. Rust checks canonical UUIDv7 values first,
verifying version and variant before extracting their millisecond timestamp.
RFC3339 timestamp fields follow. Missing or invalid coverage is reported; when
no usable timestamp exists, explicit timestamp mapping is required. Import time
is never substituted. Candidate inspection does not register or import data. Local macOS Register And
Import validates the whole file before registration; browser review does not yet
submit imports. See [format support and verification](research/ports-2026-11-21/README.md).

Sample review reports exact integer min/max as decimal strings (including
128-bit bounds), chooses the smallest 1–16-byte signed or unsigned width, and
reports UTF-8 text byte lengths with the observed maximum as proposed capacity.
Floats use four bytes only when narrowing every sampled value from `f64` to
`f32` and back preserves its bits; otherwise they use eight. Compatible mixed
integer/float samples can share a float type only if the integers are exactly
representable. Float overflow and underflow to zero are rejected.

Potential enums are provisional repeated integer/text values with occurrence
counts. The preview requires at least four non-null observations and at most
half as many distinct values; evidence is capped at 32 choices of 256 encoded
bytes each per field. Recognized UUIDv7/timestamp fields are excluded. These
are inspection heuristics, not enum restrictions in the generated candidate.
Validate every imported value against the reviewed Definition; sample ranges
and choices do not establish limits for future events.

Before changing ownership, run `../snoot/desktop/tests/baseline.py --output <new-directory>`
against the packaged app and the indexed sandbox fixture. It preserves artifact
and dataset hashes, exact queries/window, fresh-replica and warm timings, failed
sessions, and native activity cleanup. It does not test first launch: the existing
query harness deliberately preconfigures a connection. First-launch acceptance
must separately use a clean profile without that injected connection and prove
both installation and connection paths. Use `../snoot/desktop/tests/test_search.py
<staged-app> --unconfigured --verify-setup` for the isolated installation test,
and `--verify-branch-setup` with an explicit connection for candidate review.

The agreed CLI counterparts are `trunk connect <address>` for connecting to an
upstream and `trunk install` for provisioning a Trunk; `trunk serve <name>` starts
a configured node. Named-node initialization and invocation are not implemented
yet. The existing `trunk serve [port]` command continues to run the current server.

Storage strategy is **Pack** (`.packs`, compressed) or **Browse** (`.beds`,
uncompressed). It is independent of authoritative/replica ownership and peer
access. Either strategy can serve peers, and a node can be both a client and a
server. Node configuration supplies a default strategy; branches may select a
different strategy. Peer delivery policy must not implicitly change a branch's
stored representation or create a second persistent copy.

| Proposed Command | Purpose |
| --- | --- |
| `trunk install --name personal --storage browse` | Provision a Trunk with Browse storage by default |
| `trunk install --name primary --storage pack` | Provision a Trunk with Pack storage by default |
| `trunk connect <address> --name analysis --storage browse` | Configure an upstream and Browse storage |
| `trunk connect <address> --name relay --storage pack` | Configure an upstream and Pack storage |
| `trunk serve <name>` | Run the configured node, whether authoritative, replica, or both |

Changing storage strategy for existing data requires an explicit transition;
saving a preference must not relabel existing files. The query-telemetry service
is `trunk-client`, with the `queries` resource and reusable `log.query` Definition.

Snoot's desktop app runs the same frontend, workers, and pill-packer WASM codecs
inside a Neutralino WebKit window. A bundled Go bridge serves the frontend and
streams server requests. There is no Node.js runtime in the app.

At page startup the SharedWorker opens an authenticated binary WebSocket to the
local bridge. Resource GETs share this connection with independent request IDs,
cancellation, and bounded receive windows (eight 32 KiB chunks per request).
The bridge forwards each request to the selected Unix socket or HTTP origin.
PAC/PIL bodies and resource paths are unchanged; transport metadata never enters
the stored resource. A disconnected request fails without replay; the connection
reopens in the background for subsequent requests. Browser-only sessions continue
to use their existing HTTP transport. Desktop diagnostics label resource messages
`ws`; sent and body-complete timestamps are worker observations, and no native
HTTP first-byte timestamp is fabricated for them.

Build on macOS with Go, Python 3.12+, CMake, Xcode command-line tools, and the existing
frontend build dependencies installed:

```sh
make -C ../snoot desktop
# Repackage an already built frontend:
python3 ../snoot/desktop/build.py --skip-web
```

The default build produces `../snoot/desktop/dist/Snoot.app` and
`../snoot/desktop/dist/Snoot-macOS-universal.zip` for Apple Silicon and Intel Macs running
macOS 14 or later. The app uses the front-facing Snoot icon. Neutralino 6.10.0 is
pinned and its source checksum is verified. The build applies
`../snoot/desktop/neutralino-macos-exit.patch` to route Close, Quit, and termination signals
through Cocoa's main queue, save window state, and let Cocoa finish shutdown.
Compiled runtimes are cached by architecture and patch hash; temporary compiler
outputs are discarded. Local builds are ad-hoc signed;
public distribution still needs Developer ID signing and notarization.

The macOS host also compiles `../snoot/desktop/activity.mm` through
`../snoot/desktop/neutralino-macos-activity.patch`. A host-started asynchronous WebKit task
keeps the query worker schedulable while requests are outstanding. The task ends
on the worker's final `settled` acknowledgement, including cancelled requests;
workspace/node teardown releases outstanding leases. Navigation invalidates the
previous document's work. This adapter handles lifecycle only, without query or
event bytes. It does not disable App Nap or change WebKit's idle policy, and
browser-only clients do not acquire native leases. The runtime cache key includes
the adapter source. Background progress and first-query latency are separate
claims; the scheduling change alone does not establish a latency improvement.
Deployed-app results and the controlled suspension comparison are recorded in
[Native Activity Deployment Verification](research/benchmarks/native-activity-deployment-2026-11-17/README.md).

For an Intel-only package targeting Ventura 13.7 (including 13.7.8), build into
a separate directory so the installed universal app is preserved:

```sh
python3 ../snoot/desktop/build.py --skip-web --arch x64 --min-macos 13.7 \
  --output-dir ../snoot/desktop/dist/intel-ventura
```

The runtime deployment target and app minimum OS version both follow
`--min-macos`. Intel Go builds use the baseline AMD64 instruction set. Ventura
builds also apply `../snoot/desktop/neutralino-macos-ventura.patch` to exclude the
macOS 14-only screenshot API from their native binary. The native JavaScript
API remains disabled in Snoot. Ventura uses its installed WebKit; keep Safari
updated. Cross-compilation and binary
checks do not replace testing on an Intel Mac running Ventura.

The native lifecycle regression test launches isolated windows and exercises the
close button, Close Window, Quit, Command-W, Command-Q, repeated close requests,
and termination signals. It verifies saved window state and bridge cleanup:

```sh
python3 ../snoot/desktop/tests/test_lifecycle.py ../snoot/desktop/dist/Snoot.app
```

Launch the app normally for HTTPS, or select a local Trunk socket:

```sh
open ../snoot/desktop/dist/Snoot.app --args --socket "$PWD/caches/.trunk/search.sock"
```

A configured remote connection is saved in
`~/Library/Application Support/Snoot/connection.json`, for example:

```json
{
  "endpoint": "https://snoot.trietrunk.com",
  "socket": "/absolute/path/to/caches/.trunk/search.sock",
  "port": 34185,
  "transport": "auto"
}
```

The socket is optional. When available at startup, requests use it; otherwise
they use the configured HTTP(S) endpoint. Streaming responses, SSE, and
WebSocket upgrades pass through the bridge. A failed request is not replayed
against another server. Restart to select a different connection. Remote
endpoints require HTTPS; local HTTP endpoints are supported.

In the app, **Connection** shows the active transport and server/socket address.
Choose **Automatic**, **Unix Socket Only**, or **HTTP(S) Only**, then **Save
Connection** and restart Snoot. Automatic prefers the socket and falls back at
startup. Unix Socket Only reports an unavailable socket instead of falling back;
HTTP(S) Only always uses the endpoint. A saved choice never changes an in-flight
session's transport.

Edit `connection.json` to change addresses. `--endpoint`, `--socket`, `--port`, and
`--transport auto|socket|http`
override it for one launch. `SNOOT_SOCKET`, `TRUNK_SOCKET`, or
`TRUNK_DIR/.trunk/search.sock` supply a socket when none is configured.
Keep the loopback port stable: WebKit associates storage with that origin.
The bridge listens only on loopback and requires an application session cookie.

Events remain in OPFS, profile preferences and the bounded SearchesLRU metadata
in IndexedDB, and page/search state in session storage. Desktop storage is separate from Safari's website storage.
Neutralino does not remove WebKit's storage quotas; an adapter for native
filesystem storage remains separate work.

The packaged app can verify its native WebKit capabilities without screenshots:

```sh
../snoot/desktop/dist/Snoot.app/Contents/MacOS/snoot --self-test \
  --state-dir /tmp/snoot-check --port 34186 \
  --socket "$PWD/caches/.trunk/search.sock"
```

This checks server access, frontend loading, workers, WASM, OPFS synchronous
read/write, IndexedDB scope, and viewport overflow, then exits. Results are
written to the selected state directory as `self-test.json`. `--inspect` enables
WebKit developer tools when needed.

## PIL

PIL/PAC codecs and their authoritative specification now live in the sibling
[Pill Packer](https://github.com/authenticity-labs/pill-packer/blob/main/README.md) library. Keep it beside this checkout at
`../pill-packer`: Rust path dependencies, the Go module replacement, the local npm package, fixture tests, and the
site build use that path. The packages are not yet published. This repository owns
[Trie-Trunk's definition profile](schemas/trunk-definition.cddl), routing, storage,
query semantics, and HTTP registration. Pill Packer owns generic encoding,
decoding, definition documents, PAC factoring/framing, gzip and numeric column
transforms; it has no cache/service/resource policy. The Go codec is imported
from `github.com/authenticity-labs/pill-packer/go/codec`; browser helpers come from
`pill-packer` exports. Trunk adapters supply resource roles and TPIL control
schemas, without maintaining another PIL field encoder or PAC parser.

Summary Definitions are generated in Rust from `rust/seeds/definitions/summary.rx`
and the event fields annotated with `#summ(class)`. Each annotated field becomes
an object under its actual field name, containing the fixed members of that
summary class. Ordinary daily Packs contain 288 fixed-length five-minute Pills.
Within one summary revision, fields with the same class schema reference one
shared object Definition and compiled class plan. Field names and units remain
on the containing fields. Numeric widths, signedness, and category values/order
must agree before sharing; matching byte lengths alone are insufficient.
Mixed summary revisions use the existing PAC Definition directory and Fixed
Sparse records when nulls are required; periods are never duplicated.

Each summary revision owns a complete copy of its referenced object Definitions,
including unchanged class shapes. Those objects carry the owning revision; they
are not shared across historical revisions. Structural digests include referenced
content and remain independent of bundle-local pointer positions.

Summary lineage is registered independently on the same `trunk.trie` branch as
the event lineage. Publishing changed analytic annotations can generate a new
summary revision without changing the event schema version. Each summary PAC
carries its registered summary references in the `summary.definitions`
application header. Readers fetch and cache the separate summary `.rx` resource
and resolve the exact stored revision, including after restart. A reader never
reconstructs an old Pack's shape from current event annotations.

`GET /<cache>/<service>/<resource>/<type>/summaries?definition=<digest>`
accepts repeated `range=<begin_epoch_ms>,<end_epoch_ms>` parameters through the
existing range scheduler. Days follow the trunk's configured UTC offset. The
response is a stream of adjacent daily PACs, each with its own standard PAC
length and compression framing. `Trunk-Pack-Count` identifies the number sent.
The server streams the stored PAC bytes through the existing pooled read path.
Snoot expands compressed PACs in Rust, checks hourly counts against Time, and
publishes completion only after the existing storage transaction succeeds.

Rust summary readers supply predicate bounds, five-minute candidate periods,
histograms, and chart-sized reductions. Unsupported predicate fields and missing
coverage remain unknown; they cannot prove that Search has zero matches.
Percentiles over summaries describe the distribution of the selected interval
aggregates, not the underlying individual event values. The default dashboard's
P95 Period Mean Duration is calculated from five-minute means.

POST `/seeds` feeds accepted PILs into the existing Rust summary accumulators.
GET `/summaries` includes unsealed daily snapshots for current data; ordinary
summary queries can aggregate these snapshots without a subscription API. Open
periods remain provisional and cannot prove that a search has zero matches.
Only sealed daily Packs enter the `.summ` Box.

Recovery rebuilds unfinished summaries from committed event Packs and replays
the remaining ingress spool through the same accumulator. Daily publication
registers the generated Definition first, then stages the replacement tail in
`.summ.pack` before syncing `.summ`. Recovery replays that staged tail without
rolling back the registry. The existing range transport, pooled reads, and
summary query engine remain in use.

The HTTP integration test exercises admission, live queries, restart, hourly
commit, daily sealing, and an interrupted footer write. The resulting PACs are
also queried through the WASM reader; see
[the end-to-end results](benchmarks/ingress/2026-11-16-summaries/e2e.json).
Error/level statistics remain unavailable when their summary binding is absent.

The definition-engine registration plan is JSON folders/upload/editor → portable
structure/codec checks → Trie-Trunk profile checks → immutable PIL `.rx` → publication.
Persist definitions, including structured CDDL schemas, as PIL `.rx` documents.
The compiler/runtime and application-profile boundaries are described below.
The fixed `pil/value` bootstrap layout is built into the codec and published as
[`pil.rx`](https://github.com/authenticity-labs/pill-packer/blob/main/schemas/pil.rx);
that definition is encoded using the same layout it describes.
Keep source text separately for editor fidelity. Unknown metadata must survive
forwarding; required validation must be explicitly supported, not inferred from
an annotation. Folder ingestion and editor/upload UI are pending.

PIL carries complete objects or partial changes. Definitions specify ordered
fields, primitive types, dictionaries, and integer-encoding options. Definitions
are PIL `.rx` documents; [`definition.cddl`](https://github.com/authenticity-labs/pill-packer/blob/main/schemas/definition.cddl) is the editable CDDL description of their structure.
Root `definitions/<digest>.rx` and `definitions/producer-<digest>.rx` store
native PIL Definitions. Registration metadata retains its existing PIL encoding
inside `trunk.trie`; there is no per-cache runtime `registry.map`.
Absent, null, direct, and offset-backed field states remain distinct. `REF16` is
optional. Adaptive integers are configured per Definition.

Trie-Trunk implements PIL records as data seeds and PIL PACs as seed packs.
A seed pack groups data seeds under one Definition and may factor shared values. Its
readable compressed wrapper describes the packed body; decoding is owned by the
client library rather than HTTP content decoding. Storage placement and query
execution belong to Trie-Trunk, not the interchange format.

See [`../pill-packer/rust/src`](https://github.com/authenticity-labs/pill-packer/tree/main/rust/src), [`../pill-packer/rust/tests`](https://github.com/authenticity-labs/pill-packer/tree/main/rust/tests), and the cross-language fixtures
for the implemented format. Format experiments replace fixtures; old benchmark
snapshots are not compatibility contracts.

### Definition Validation Ownership

Pill Packer compiles supported CDDL into a PIL root layout and nested schema plan.
Rust, Go, and WASM share that nested runtime and reject a schema that disagrees
with its compiled layout. Trie-Trunk separately validates resource identity,
version, timezone, role/type bindings, aggregation, and shared projection paths
in `go/server/cache/definition.go` and `references.go`.

Compiler, CLI, bootstrap, and binding parity tests live in Pill Packer's
`rust/tests/schema.rs`, `rust/tests/rx.rs`, `rust/tests/documents.rs`,
`go/codec/schema_test.go`, and npm tests. Cache profile checks stay in Trie-Trunk's
cache tests; demo-generation tests stay with `go/cmd/seed-demo`.

Demo Definitions are still assembled as Go structs. Loading authored `.rx`
resources requires an explicit portable binding for CDDL constraints to storage
roles: for example, `bstr .size 16` compiles as BYTES while the cache identity role
requires UUID. Neither validator should be weakened to disguise that gap.
Required named and inline CDDL map groups flatten into one layout while preserving
member optionality. Optional/repeated groups and group choices are rejected.
Generic/recursive rules, arbitrary dynamic map keys, tuple arrays, and unbounded
`int`/`nint` remain outside the supported CDDL subset. Folder ingestion
and the Definition upload/editor UI remain pending.

Authoring schemas live in [`schemas/log.cddl`](schemas/log.cddl) and
[`schemas/collection.cddl`](schemas/collection.cddl). Log roots use the
OpenTelemetry Logs Data Model with HTTP, database, or messaging attributes.
Collection identity remains separate from logs. `RX=/path/to/rx schemas/build.sh`
compiles each root into one `.rx`; the standard CDDL groups add no runtime
inheritance. These authoring schemas do not yet replace the storage-bound
request logger Definition described below.

## Logger

The Go PIL logger registers a Definition before its first write. It exchanges
identity and digest, sends missing maps, and validates a compatible server layout.
`WriteValues` encodes positional values into the accepted layout; `WriteFrames`
accepts frames under the producer's original Definition. Extensions can use
explicitly enabled `@fields` and be promoted by compatible newer Definitions.

```go
mapBytes, err := os.ReadFile("resource.rx")
if err != nil { return err }
logger, err := pil.Open(ctx, pil.Config{
    URL: trunkURL, Cache: "prod", Branch: "write",
    Token: token, Definition: mapBytes,
})
if err != nil { return err }
defer logger.Close()
return logger.WriteFrames(ctx, frames...)
```

Import `trie-trunk/go/logger/pil`. The example assumes application-supplied
context, endpoint, token, and PIL frames. For HTTP instrumentation, use
`client.NewPIL`; see the [runnable example](go/logger/client/examples/http/main.go).

`BatchMilliseconds` optionally batches concurrent producers; zero sends
immediately. HTTP gzip compresses batches before ingress decompression. Revisions
are preserved without coalescing. Calls wait for staging acknowledgement rather
than succeeding on local queue insertion. Observe delivery errors and retain
stable event identities for retries. See [licensing](#licensing) for the current
SDK dependency-boundary caveat.

### Immutable Memory Measurements

[Before/after samples](benchmarks/server/2026-11-01-immutable-memory.json) use the
saved working tree from before the immutable-memory edits, identical fixtures,
and three 200 ms runs on the same machine. These are local handler/API timings,
not browser latency. For 10,000 events in one sealed hour, median PAC delivery
was 8.93 → 8.44 µs serial and 10.10 → 9.08 µs parallel. Native payload buffer
capacity was 1,547,432 → 105,640 bytes; this excludes Go heap, indexes, allocator
metadata and process RSS.

Removing duplicate expanded PILs trades memory for reconstruction during sealed
object-history reads: medians for 1/9/61 revisions were 1.00/1.50/4.18 µs before
and 2.50/11.02/57.11 µs after. Reconstruction is reader-owned, outside the writer
mutex, and resident PAC access does not read the payload file. Pending history
and expired-history disk paths remain separately measured in the report.

### Server Resource Logs

`trunk serve` enables `--server-log` by default. The server uses the Trie-Trunk Go logger's request builder
to register separate Definitions for `trie-trunk/definitions`, `registries`,
`timelines`, `seeds`, `summaries`, `indexes`, `leaves`, and `collections` in the
configured cache. It publishes to the serving process's Unix-socket HTTP listener
through the ordinary `/ingest` path, with the configured authentication token.
It never inserts observations directly into a Store or a telemetry event queue.

Each completed resource GET starts a direct listener delivery independently of
response completion. Logger batching and transport compression are disabled.
Delivery failures go to stderr; shutdown waits for outstanding deliveries.
Registration and ingress requests are excluded to prevent recursive logging.
The builder receives an explicit `INFO` level for normal requests and `ERROR`
for HTTP failures. Handlers can enrich the same builder through their request
context, including `DEBUG` and `TRACE` levels. The logger owns branch routing:
normal GET completions use `read`, errors use `error`, and diagnostic levels use
their corresponding diagnostic branches. The `level` and `access` fields remain
separate, so a failed read retains `access=read`.

Each resource uses the logger's standard Definition, including `event_id`,
`level`, `service`, `resource`, `http_method`, `http_status`, `uri`, `duration`
(U64 microseconds), `response_bytes`, and `access`. Declared `protocol` and
`selection` string fields carry transport and accessor details. Selection
includes only time/range/row/day/hour/kind accessors. Tokens and arbitrary query
text are excluded. The builder records the handler's completion timestamp before
asynchronous delivery, so delivery scheduling does not inflate `duration`. These are
request observations, not a measurement of the browser's full search duration.
Use `--server-log=false` to disable them.

## Validation

```sh
make test                        # native Rust/PIL and Go suites
cargo test --locked --all-features --manifest-path ../pill-packer/rust/Cargo.toml
npm --prefix ../snoot/client test         # codec, store, query and UI logic tests
npm --prefix ../snoot/client run check    # TypeScript
node site/build.mjs
```

Browser/OPFS probes under `../snoot/client/scripts` are separate from the unit suites.
They require a supported browser runtime; a passing CLI build does not establish
browser visual correctness, Safari storage behavior, or production performance.

## Measurements

### Ordinal Identity Format (Development)

Ingress takes the timestamp from the supplied UUIDv7 and discards its entropy.
`@event_id` is a provisional per-hour number while live; sealing sorts the hour and
assigns final zero-based U24 row numbers (0–16,777,215). These numbers are scoped by
resource, physical branch, and hour. They are not globally unique. The original
UUID cannot be reconstructed and UUID-based ingress retry deduplication is no
longer available. Collection object/revision uniqueness remains enforced.

`@leaf` remains a REF16 Definition field, but its stored value lives only in the
two-byte index postfix. State values covered by that leaf are also omitted from
the sealed member and resolved from the state trie. Materialized event views
expose those values and `@leaf` without creating a second persisted copy. The
original UUID field is hidden in ordinal event views; internal live/paging APIs
use a synthetic timestamp/ordinal key, not the producer UUID.

Collection index encoding is unchanged: event positions start at one, and zero
remains invalid/empty. The boundary converts a zero-based `@event_id` to the
existing one-based collection position. Hour values, page layout, independent
width widening, and retention addressing keep their established conventions.

[Ordinal benchmark results](benchmarks/storage/2026-10-29-format-comparison/ordinal-results.json)
use the same 1,048,576 input events and 256 hours as the original baseline.
The resource store includes summaries and excludes Tantivy and collection indexes.
The client replica covers the event/index replication path; summaries are not
part of that client measurement. All bytes below are decimal MB.

| Measurement | Original Baseline | Entropy Experiment | Ordinal Layout |
| --- | ---: | ---: | ---: |
| Total Server Store | 22.59 MB | 27.76 MB | 15.81 MB |
| Total Client Replica | 39.04 MB | 46.47 MB | 35.99 MB |
| Client Decode | 98.8 ms | — | 147.1 ms |
| Client Decode and Durable Replication | 420.5 ms | — | 579.6 ms |
| Local 100-Event Page | 21.4 ms | — | 27.7 ms |

The original and ordinal builds were measured sequentially in the same isolated
Chrome session: seven decode runs, five storage-worker/OPFS replication runs, and
five page reads per replication. Timings exclude network transfer and DOM/GPU
rendering. All 256 hours and 1,048,576 events passed full readback. Original UUID
hashes intentionally differ because this design discards entropy.

The footprint is smaller, but retrieval is still slower than the original
baseline. The [member audit](benchmarks/storage/2026-10-29-format-comparison/ordinal-audit.json)
finds 22-byte decoded members, including the fixed three-byte ordinal and no
payload copy of `@leaf` or trie-covered state. Four-byte PAC directory entries
remain, along with a three-byte mask versus the original two-byte mask. This
fixture has uniform member widths; directory elimination remains an optimization
target. No speedup in total retrieval is claimed.

### Entropy-Preserving Experiment (Superseded)

[Reproducible results and artifact hashes](benchmarks/storage/2026-10-29-format-comparison/results.json)
compare Trie-Trunk `42ebf75` and Pill Packer `e663e89` with the earlier entropy-preserving
experiment. Both use the same 1,048,576 deterministic events in 256 hours, including
full 74-bit UUID entropy. Summaries are enabled; Tantivy and the optional
collection index are disabled in both. These measurements cover the full isolated
resource store, not just event payloads.

| Measurement | Before | Development |
| --- | ---: | ---: |
| Event index | 16.78 MB | 6.29 MB |
| Total server store, logical bytes | 22.59 MB | 27.76 MB |
| Total client replica, logical bytes | 39.04 MB | 46.47 MB |
| Time lower-bound lookup, 1 million rows | 176 ns | 146 ns |
| Time lower-bound lookup, 16 million rows | 481 ns | 365 ns |
| Client decode | 95 ms | 222 ms |
| Client decode and durable replication | 416 ms | 701 ms |
| Local 100-event page with field materialization | 21.5 ms | 27.2 ms |

Client medians use five alternating Chrome runs through the actual storage
worker and OPFS. Transfer bytes are already in memory when timing starts;
network download and DOM/GPU painting are excluded. All million records are
read back, and reconstructed UUID hashes agree. Go lookup runs use five samples
per format and zero allocations per lookup.

In that experiment, the index was smaller and faster, but the entire change was
not a performance improvement. Relocated entropy passed through gzip; that PAC layout
also introduces directory entries, variable-field offsets, and a wider field
state mask. Controlled same-byte decompression is approximately 144 ms with the
old Pill Packer and 142 ms with the new one, so these results do not show a slower
decompression codec. The report includes entropy/directory isolation measurements.
A [full member audit](benchmarks/storage/2026-10-29-format-comparison/after-audit.json)
accounts for the replica growth: 10 fewer index bytes, but 13 more member bytes
and four directory bytes per event. All measured members are exactly 32 bytes,
so this fixture does not need a directory. The state field is also still present
in the payload despite its leaf being indexed. Original UUID and `@time` values
are absent from sealed members; entropy is stored once. This is an implementation
overhead finding, not evidence of duplicated complete event copies.

An idle native-buffer suffix saturation bug found during restart benchmarking
has been fixed and covered by a bounded-memory regression test.

To regenerate the development fixture in an isolated temporary store:
`TRUNK_FORMAT_BENCH_OUTPUT=/tmp/trunk-format/dense.pil go -C go test ./server/cache -run '^TestFormatComparisonFixture$' -count=1 -v`.
Run index measurements with `go -C go test ./server/cache -run '^$' -bench '^BenchmarkFormatIndex$' -benchmem -benchtime=500ms -count=5`.


**The full Trie-Trunk datastore is 70.9% smaller than expanded JSONL** in the
million-event synthetic fixture, including indexes, summaries, journals, and
metadata. This historical measurement used compressed writes with Collections
disabled; it predates the current uncompressed `write.packs` policy.

| Artifact | Decimal MB |
| --- | ---: |
| Expanded JSONL | 427.75 |
| Complete Datastore, Including Indexes and Summaries | 124.65 |

The separate codec comparison uses identical hourly boundaries for both formats.
Seed packs retain complete UUIDs and routing context, with definitions and packet
headers included; no external index is needed to decode them.

| Compression | Hourly JSONL | Hourly Seed Packs | Less Storage |
| --- | ---: | ---: | ---: |
| Gzip-1 (Default) | 37.09 MB | 19.98 MB | 46.1% |
| Gzip-6 | 31.36 MB | 17.98 MB | 42.7% |
| Gzip-9 | 29.67 MB | 17.93 MB | 39.6% |

Compression/decompression time for all one million events, median of three serial
runs on Apple M1 Max (Go 1.26.4, one worker):

| Setting | Format | Compress | Decompress |
| --- | --- | ---: | ---: |
| Gzip-1 | JSONL | 1.04 s | 0.60 s |
| Gzip-1 | Seed Packs | 0.84 s | 0.33 s |
| Gzip-6 | JSONL | 2.05 s | 0.50 s |
| Gzip-6 | Seed Packs | 1.83 s | 0.24 s |
| Gzip-9 | JSONL | 3.40 s | 0.48 s |
| Gzip-9 | Seed Packs | 3.33 s | 0.24 s |

The fixture spans 30 days and five resources, with repeated messages and dictionary
values. It cycles through 250 object IDs per resource, not one million independent
objects. Every event was read back and every standalone seed pack was round-trip
verified. Production results depend on the data distribution. The 8 GiB native
budget per resource is a bulk-test limit, not measured RSS.

Gzip-1 is the default; levels 6 and 9 are configurable. Gzip is currently the
supported compressor. Timing results measure compression/decompression separately
from PIL packing, JSON serialization, disk I/O, and queries.

Raw accounting and measurements:
[`benchmarks/storage/2026-10-26-million-events-standard`](benchmarks/storage/2026-10-26-million-events-standard).
Generated exports, packet snapshots, and codec inputs remain local.

```sh
export TRUNK_FOOTPRINT_OUT="$PWD/benchmarks/storage/2026-10-26-million-events-standard"
TRUNK_FOOTPRINT_EVENTS=1000000 TRUNK_FOOTPRINT_COLLECTIONS=false TRUNK_FOOTPRINT_NATIVE_BYTES=8589934592 go -C go test ./cmd/seed-demo -run '^TestDemoFootprint$' -count=1 -timeout=60m -v
TRUNK_SUMMARY_MATCHED="$TRUNK_FOOTPRINT_OUT" go -C go test ./server/cache -run '^TestSummaryMatchedFootprint$' -count=1 -v
TRUNK_COMPRESSION_MATCHED="$TRUNK_FOOTPRINT_OUT" go -C go test ./cmd/seed-demo -run '^TestStandalonePACCompression$' -count=1 -v
TRUNK_COMPRESSION_TIMING="$TRUNK_FOOTPRINT_OUT" TRUNK_BENCHMARK_CPU="$(sysctl -n machdep.cpu.brand_string)" go -C go test ./cmd/seed-demo -run '^TestHourlyCompressionTimings$' -count=1 -timeout=10m -v
```

### Retrieval API Measurements

The October 26 retrieval run uses a **native Go client against the running
Go/Rust server**, with the 50,000-seed demo across five resources and 25 branches.
The query window is September 25–October 26 UTC. The machine is an Apple M1 Max
with 32 GiB RAM. This is separate from the million-event storage fixture above.

Public-path results below verify HTTP/3 on every response. Each row has ten
measured trials after one discarded setup trial. Server/OS caches are warm and
uncontrolled; each trial downloads fresh summary and seed-pack bodies. p95 uses
nearest rank and is the maximum of these ten observations, not a production SLA.

| API Workload | Scheduling | Median | p95 | Requests |
| --- | --- | ---: | ---: | ---: |
| 100 Seeds + Full-Range Summaries | Baseline: Four Requests per Stage | 9.87 s | 10.28 s | 481 |
| Same Seeds + Summaries | Experiment: Sixteen Requests per Stage | 2.97 s | 3.11 s | 481 |
| Same Seeds + Summaries | Experiment: Thirty-Two Requests, Overlapping Stages | 1.86 s | 2.06 s | 481 |
| 100 Seeds Only | Four Requests | 542 ms | 649 ms | 21 |

The scheduling experiments are benchmark options, **not deployed optimizations**.
All full-view runs returned the same selected seed identities. The 481 requests
comprise one search, 460 daily summary packets, and 20 hourly seed packs. Daily
packets belong to individual branches: five resources × (31 Info–Read days +
31 Info–Write days + 30 Error days). The first 100 seeds span two hours across
five resources and two branches. These requests transfer about 0.90 MB of HTTP
response bodies, excluding protocol headers/framing.

Local API delivery medians were 129 ms for seeds plus summaries, 60 ms for seeds
alone, 343 ms for a broad numeric query, and 468 ms for a broad text query.
The numeric and text queries spent roughly 330 ms and 401 ms, respectively, in
the initial search response. One-branch search responses were about 25 ms.
The scopes and matching data sets differ; this comparison locates work rather
than claiming an equivalent-workload speedup.

Measurements include HTTP gzip decoding, routing metadata parsing, and summary
SHA-256 verification. They exclude seed-pack application decoding, SharedWorker
messaging, OPFS writes, chart calculation, and painting. No native coverage
receipts are sent between seed-pack downloads, so overlapping dependency bytes
may be transferred again. The separate `KnownMetadataMS` metric is one request
with cache receipts, not a complete warm-view time. No Node runtime is used in
this benchmark. No competing platform has been measured on this workload yet.

The browser now delivers the validated event page before loading chart summaries
from the same snapshot. Missing daily summaries are requested by exact branch and
Definition using repeated UTC-day `range` parameters on the branch-scoped `/summaries` endpoint.
The response is a streamed PAC of unchanged daily PACs in ascending day order.
There is no JSON request body, separate payload envelope, or outer recompression.
Stored day files are pinned before response publication. Snapshot consumers verify
content identities; time-trie replication checks generation and coverage before
saving. Truncated or corrupt children never publish completion. Complete children
already committed remain usable on retry. Client parsing awaits each pack before
reading the next, and background priming awaits persistence. Seven-day subscription
windows remain scheduling units, rather than seven separate HTTP responses.
Compressed summary storage
uses a 256 MiB byte budget. Search cache writes run off the foreground path with
an 8 MiB queue bound; explicit subscriptions still await persistence. The event
table mounts a measured viewport plus overscan. JSON details are generated only
for an expanded event, never for every loaded row.

An isolated Chrome check on the server Mac, without CPU/network throttling,
measured the same 50,000-match search with a 100-event first page:

| Measurement | Before | After |
| --- | ---: | ---: |
| Cold event data ready (one run) | 9.98 s | 1.32 s |
| Cold chart data ready (one run) | 9.98 s | 3.16 s |
| Warm next-frame proxy (median of three refreshes) | 269 ms | 117 ms |
| Mounted events at the top of the table | 100 | 8 |
| Page DOM elements | 9,046 | 1,257 |

The next-frame proxy uses two animation frames after the row DOM appears; it is
not an INP measurement. Network wait varied between runs, so these are local
before/after observations, not a cross-platform benchmark. Search timing now
reports event delivery; the separate Charts timing reports chart-data readiness
from the same search start. Both exclude painting. Raw browser measurements are
in [`browser-optimization.json`](benchmarks/retrieval/2026-10-26-demo-audit/browser-optimization.json).

The batched summary transport was checked separately on the same 50,000-seed
fixture (100-seed page, 460 daily packs, 20 hourly seed packs):

| Batched API Delivery | Median, Three Trials | Summary Requests | Total Retrieval Requests |
| --- | ---: | ---: | ---: |
| Local HTTP | 74.6 ms | 1 | 22 |
| Public HTTP/3 | 785.0 ms | 1 | 22 |

Each run discards one setup trial, uses a persistent transport, validates every
summary digest, and excludes the separate known-metadata probe from retrieval
counts/timing. Server and OS caches were warm and uncontrolled. These are later
observations rather than a controlled speedup comparison with the earlier runs.
The batching deterministically replaces 460 summary requests with one, while
preserving the pack contents. Results: [local](benchmarks/retrieval/2026-10-26-demo-audit/summary-batch-local.json)
and [public HTTP/3](benchmarks/retrieval/2026-10-26-demo-audit/summary-batch-public.json).
An isolated Chrome reload with its summary cache cleared also populated all 460
packs, rendered 50,000 matched seeds, and reported no errors; its event cache was
retained, so this is a functional check rather than a fully cold benchmark.

### Hourly Client Decode and Storage

Measured in Chrome on the Mac on October 27, 2026. These timings start with a
response already in worker memory; they exclude download and UI rendering.
The client uses pill-packer's Rust/WASM scanner, PAC decoder, decompressor, and
CRC32C verifier. Native hourly PACs are checked against their sealed manifest
checksum; member boundaries come from the checksum-verified event index. Reception
does not revalidate event field values. Definitions are compiled once per digest
in a bounded cache; binding context remains separate. Structural bounds, source
binding, and generation checks remain active. Full validation APIs remain available
for ingestion and diagnostics. The storage worker reuses the transport worker's
internal integrity result, then flushes `.beds` and indexes before publishing
committed time-trie leaves.

Sealed native collection now coalesces adjacent delivery windows and streams
contiguous `.packs` byte ranges from pinned file descriptors. Time-trie endpoints
locate the bytes without consulting event index rows. An empty or blank endpoint
walks backward to the preceding populated sibling and reads that PAC's length;
the trie layout is unchanged. Address lookup does not certify coverage: every
confirmed empty hour remains in the replicated trie patch, while blank hours
remain unknown. Index sidecars are copied without decoding, and the client retains
per-PAC integrity checks. HTTP range framing (TPIL tag 9, stream U32 and byte length
U64, then unchanged consecutive PAC bytes) avoids per-pack response markers,
payload allocations, length reads, and unused server SHA-256 calculations.

The in-memory directory uses approximately 143 KB per calendar year per opened
physical branch at default address widths (about 1 MB for seven years). Wider
fields, calendar boundaries, and publication copies add overhead; there is no new
directory copy per PAC. Bounded residency remains a future scaling consideration.

| Workload | Before | After |
| --- | ---: | ---: |
| 721 sparse hours / 6,232 events, decode and durable storage | 10.865 s | 0.465–0.527 s |
| Same sparse workload, file opens / flushes | 2,890 / 2,889 | 5 / 5 |
| 256 compressed hours / 1,048,576 events, decode only | 165–174 ms | 102–115 ms |
| Million-event decode and durable storage through production worker port | 705–784 ms | 482–561 ms |

The sparse baseline is one observed run; optimized ranges are three cold local
storage trials. Dense fixtures contain 4,096 events per hour and use the real
server sealer; fixture generation bypasses ingress and disables full-text index
construction. Every payload byte, event identity, and member boundary was read
back and compared for both fixtures. Failure-injection tests ensure failed
writes/flushes cannot publish trie leaves. These are client measurements, not
server ingest benchmarks or physical iPhone results.

Raw trials and codec/fixture hashes:
[hourly client results](benchmarks/retrieval/2026-10-27-hourly-client/results.json).
The follow-up [checksum/indexed reception results](benchmarks/retrieval/2026-10-27-structural-decode/results.json)
use the same million-event fixture. Its 256 PAC checksum checks take approximately
4 ms. Sparse production-worker storage remains 465–486 ms. All 1,048,576 dense
events and 6,232 sparse events were read back after timing and compared byte for
byte; corrupt input and failed storage cannot publish affected hourly trie leaves.
The opt-in fixture generator is `TestClientHourlyBenchmarkFixture` in
`go/server/cache/client_benchmark_test.go`, selected by `TRUNK_CLIENT_BENCH_OUTPUT`.
`node ../snoot/client/scripts/build-native-benchmark.mjs` builds the isolated worker at
`/bench-native/worker.js` for the existing Go static server. It accepts `prepare`
(with `tap/from/to` or a fixture URL), `decode`, and `write` messages carrying an
`id`; `ipc: true` uses the production storage-worker port and `verifyAll: true`
checks every hour after the timed write. Benchmark OPFS namespaces are separate
from user data. Remove `/bench-native/` and its benchmark namespaces after use.

With contiguous range delivery, the same million-event client workload measures
406–416 ms for decode plus durable storage (97–113 ms decode only). The sparse
month response shrinks from 1,282,141 to 843,074 wire bytes and stores in 404–407 ms.
Both workloads pass complete payload, identity, and member-boundary readback.
The local server comparison uses 256 packs / 65,536 events: approximately 1.03 ms
with per-pack reads and checksums versus 0.63 ms with range streaming. It excludes
network transmission. Raw measurements and source hashes are in
[PAC range results](benchmarks/retrieval/2026-10-27-pac-ranges/results.json);
`BenchmarkPACRangeDelivery` reproduces the server comparison.

Remaining optimization targets:

1. Prioritize foreground summary requests across concurrent subscriptions and
   tabs; measure the deployed batched transport under concurrent load.
2. Investigate Tantivy handle churn: the Rust registry permits 16 idle cached
   sources while this demo searches 25 branches sequentially. Verify opens and
   evictions before separating reader residency from writer budgets.
3. Coalesce native-store maintenance around collection batches while preserving
   flush, publication, and retention ordering.
4. Profile filtering, candidate materialization, and cross-branch merging with
   server phase timings. Broad queries currently evaluate branches sequentially.
5. Measure sustained scrolling and mobile Safari on physical devices. Loaded
   event bytes remain available for scrolling; windowing bounds rendered rows,
   not the accumulated result-byte cache or session checkpoint size.

Raw compressed samples, protocol evidence, source hashes, and the complete table:
[`benchmarks/retrieval/2026-10-26`](benchmarks/retrieval/2026-10-26).

```sh
go -C go test ./cmd/bench-retrieval
go -C go build -o /tmp/trunk-bench-retrieval ./cmd/bench-retrieval
/tmp/trunk-bench-retrieval -trials 10 -out /tmp/retrieval-local.json
/tmp/trunk-bench-retrieval -origin https://snoot.trietrunk.com -protocol 3 -scenario branch-full -trials 10 -out /tmp/retrieval-h3.json
# Scheduling experiment only:
/tmp/trunk-bench-retrieval -origin https://snoot.trietrunk.com -protocol 3 -scenario branch-full -parallel 32 -overlap -trials 10 -out /tmp/retrieval-h3-parallel.json
python3 benchmarks/retrieval/2026-10-26/summarize.py
```

## Licensing

Copyright 2026 trie-trunk contributors.

This repository uses different licenses for the application and the reusable
codecs and client libraries. These are component licenses, not a choice of
license for the entire repository.

| Component | Paths | SPDX identifier | License text |
| --- | --- | --- | --- |
| trie-trunk server, CLI, search engine, browser store, and other repository-owned material outside the exceptions below | Repository default, including `go/server/`, `go/cmd/`, `rust/`, and root build scripts | `AGPL-3.0-only` | [GNU AGPL version 3](LICENSE) |
| PIL codec, its WASM wrapper, specification, examples, tests and benchmarks | `../pill-packer/` | `Apache-2.0` | [Apache License 2.0](https://github.com/authenticity-labs/pill-packer/blob/main/LICENSE) |
| Shared Go encoding, dictionaries, schemas and UUID helpers | `go/codec/` | `Apache-2.0` | [Apache License 2.0](go/codec/LICENSE) |
| Go logger client, outbound transport, definitions, examples and tests | `go/logger/` | `Apache-2.0` | [Apache License 2.0](go/logger/LICENSE) |
| Shared Go error models | `go/models/` | `Apache-2.0` | [Apache License 2.0](go/models/LICENSE) |

Directory licenses apply recursively unless a file or a more specific component
explicitly states otherwise. Rust manifests and Snoot's package metadata identify
their component licenses. Generated code and binaries retain the licensing
obligations of their source; a generated asset's destination directory does not
relicense it. In particular, `pil.wasm` is built from Apache-licensed PIL, while
`trunk-store.wasm` includes the AGPL-licensed browser store.

Both licenses permit commercial use. The AGPL governs covered derivatives and
includes a source-offer obligation to remote users of modified network versions.
It does not prohibit competing forks or require contributions to this repository.
See the license itself for the full terms, including distribution obligations.

Independent clients and wrappers can use separately chosen licenses. Using the
Apache-licensed SDKs or PIL codec does not by itself place an independent
application under the AGPL. Copying or combining AGPL implementation code may
create a covered work; a wrapper or plugin label does not determine that question.
Current implementation caveat: `go/logger/pil` imports the compiler and types
from `go/server/cache`, so that SDK dependency graph currently includes AGPL
server code. Component labels do not remove dependency licensing obligations.

Third-party dependencies retain their own licenses and notices. These component
licenses do not replace third-party terms or remove existing notices. Retain the
applicable license texts and notices when redistributing components.

The [trademark policy](#trademark-policy) addresses product names and logos separately.
It adds no restriction to the software licenses' permission to use, modify,
redistribute, or fork the code.

Source repository: https://github.com/authenticity-labs/trie-trunk

## Trademark Policy

The trie-trunk and Snoot names and project logos identify the project and its
official releases maintained by AuthenticityLabs. The software licenses grant
copyright and other expressly stated rights; they do not grant general rights
to use these marks as your own product identity or to imply endorsement.

You may use the names to describe the project accurately, credit its origin,
document compatibility, discuss or review it, or identify an unmodified official
release. For example, "Acme Tools for trie-trunk" and "Compatible with PIL"
are acceptable factual descriptions when they do not imply official sponsorship.
Use of a protocol's name to identify compatibility is not a claim of endorsement.

Independently maintained forks, modified distributions, extensions, wrappers and
hosted products should use their own primary product names and branding. They
may accurately state that they are based on, integrate with, or are compatible
with trie-trunk or Snoot. Clearly distinguish your product and maintainer from
the official project. Do not use the project logos as your own product logo or
describe an independent product as an official release without permission.

Keep required copyright, license and attribution notices when changing branding.
This policy does not prohibit forks, competing implementations, commercial use,
or redistribution allowed by the software licenses. It does not limit fair use,
nominative use, or other rights granted by applicable law, and does not claim
that a mark is registered.

For branding permission beyond the uses above, contact the project maintainers
through https://github.com/authenticity-labs/trie-trunk.

Dashboard statistic values fit their available card width, including space for
trend indicators. Full numbers stay on one line; font sizing is recalculated only
when the value, available width, or loaded font changes.

### Definition Presentation And Shared Leaf Labels

Snoot autocomplete reads field names, types, widths, and annotations from the registered RX. Application annotations remain visible in Data Design; query completions show the field name and its semantic type without annotation syntax. `@event` and `@log` identify UUIDv7 event identities. `@object` identifies a full UUID for PODS. `@leaf` describes the shared state reference. Declared duration widths are preserved; microsecond fields identify that unit in completions.

`trunk.trie().branches().select([branch]).leaves()` exposes `labels(bytes)` and `label(bytes, leaf, text)` (null removes a label). Labels belong to the selected Definition root in `<type>.v<major>.leaf`, separate from immutable state node bytes. Label changes preserve node offsets and are included in the ordinary leaf-file replication and ETag.

`GET /<cache>/<service>/<resource>/leaves?labels=1` returns the selected root's label map. `PATCH .../leaves?leaf=<id>` accepts UTF-8 text; an empty body removes the label. Both require `Trunk-Branch`; PATCH also requires the current `If-Match` ETag. Publication uses the existing per-type writer lock and an atomic file replacement. Stale edits return 412. Snoot exposes this editor on the event's leaf field and updates matching displayed labels after a local edit. Other nodes receive changes through ordinary metadata refresh, not a live subscription.

Configure an ordered state path in the branch registration, for example
`"path": ["region", "site"]`. At seal the Rust node interns each present,
non-null prefix in the matching Definition root, omits that prefix from the
stored PIL and writes its leaf ID in the hour index. Uncovered fields remain
in the PIL. The node publishes new leaf IDs before the PAC/index checkpoint.
Queries and unfinished-day summary recovery read the same logical values.

An independent DOC-authored HTTP log profile is provided in `schemas/logs/log.http.rx`, generated by `rust/seeds/examples/http_definition.rs`. It uses unprefixed names such as `level`, `status`, and `duration`, with `duration: uint(4)` in microseconds. It is not automatically registered and does not replace existing Definitions.

The profile maps semantic fields to [OpenTelemetry's Logs Data Model](https://opentelemetry.io/docs/specs/otel/logs/data-model/): `level` to SeverityNumber, `level_text` to SeverityText, `message` to Body, and trace/span IDs to trace context. This is a PIL Definition, not an OTLP converter. It uses millisecond timestamps, whereas OpenTelemetry timestamps use nanoseconds; a future adapter must explicitly convert units. Severity remains `uint(1)` so the 1–24 values and unspecified 0 are representable.

Known field mappings are available through Rust `definition.metadata(ordinal)` and
`trunk.describe(id).fields[].metadata`, including the WASM API. Explicit
`@otel(SeverityNumber)`, `@otel(http.response.status_code)`, and
`@otel(http.request.method)` annotations select shared profiles; field names and
namespaces alone do not assign semantics. These annotations remain design metadata.

Profiles contain canonical value/label pairs, inclusive category ranges, provider
field names, and source references. Severity retains all 24 levels and six groups;
HTTP statuses use [IANA descriptions](https://www.iana.org/assignments/http-status-codes/)
and response classes. [HTTP methods](https://www.iana.org/assignments/http-methods/)
include safe/idempotent properties. These open mappings do not restrict accepted
values, replace Definition enum dictionaries, change integer widths, or route
events. Unrecognized codes and custom methods remain intact. Conflicting mappings
across selected Definitions are omitted from shared autocomplete suggestions.

Pipeline, Builder, and SQL autocomplete display these labels and insert the original
typed literal: selecting `404 — Not Found` inserts `404`, while `GET` inserts a
quoted string. A severity group is a range, not an alias for equality with its first
value. No record bytes or Definition files change when these labels are resolved.
