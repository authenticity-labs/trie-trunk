export type SeedCast={target:'int'|'uint'|'float'|'bool';bytes:number;tolerant?:boolean};
export type QueryContext={referenceMs?:number;timezone?:string;dateOrder?:'mdy'|'dmy'};
export type SeedBytes = BufferSource | WebAssembly.Module;
/** Synchronous host I/O into Rust-owned memory. The callback must fill the
 * supplied range and must not call back into this runtime or retain the view. */
export interface SeedInputReader {
  readonly byteLength: number;
  read(destination: Uint8Array): number;
}
export type SeedInput = BufferSource | SeedInputReader;
export type SeedObject = Record<string, unknown>;
export type SeedPredicate =
  | { field: string; op: string; value: unknown; cast?:SeedCast }
  | { and: SeedPredicate[] }
  | { or: SeedPredicate[] }
  | { not: SeedPredicate };
export interface SeedHandle { dispose(): void }
export interface SeedPack extends SeedHandle {
  query(additional?: Iterable<SeedReader | SeedPack>): SeedQuery;
  /** Independently owned handle to the same immutable Rust Pack; no byte copy. */
  retain(): SeedPack;
  withIndex(bytes: SeedInput, hour: number | bigint, leaves?: SeedLeaves): SeedPack;
  info(): {length: number; byteLength: number; sealed: boolean; hour: number | bigint | null};
  /** Copy the existing decoded Pack representation; never builds or seals. */
  bytes(): Uint8Array;
}
export interface SeedDefinition extends SeedHandle {
  /** Analytic requests from annotations; no accumulator is enabled by these. */
  analytics(): {ordinal:number;field:string;percentiles:number[]}[];
  open(bytes: SeedInput): SeedPack;
  openStored(bytes: SeedInput, maximumBytes?: number): SeedPack;
  receive(maximumPackBytes: number): SeedReceiver;
  /** Bounds each encoded and expanded Pack before publication. */
  receiveStored(maximumPackBytes: number): SeedReceiver;
  /** Check incoming integrity before a storage write, then open bounded Packs. */
  receiveForWrite(maximumPackBytes: number): SeedReceiver;

}
export interface SummaryWindow {
  requested:{from:number;to:number};from:number;to:number;
}
export type SummaryAggregate = "count"|"null_count"|"absent_count"|"min"|"max"|"sum"|"mean"|"true_count"|"false_count"|"true_proportion"|"distinct";
export interface SummarySeries {
  field:string|null;aggregate:SummaryAggregate;
  points:{from:number;to:number;value:unknown}[];
}
export interface SeedSummaryPlan extends SeedHandle {
  open(bytes:SeedInput,firstPeriod:number):SeedSummaries;
  pack(bytes:SeedInput,firstPeriod:number):SeedSummaries;
}
export interface SeedSummaries extends SeedHandle {
  info():{day:number;from:number;to:number;sealed:boolean;hours:{time:number;count:number}[]};
  rows(from:number,to:number):{time:number;count:number|bigint;fields:Record<string,unknown>}[];
  query(from:number,to:number): {
    span(milliseconds:number): ReturnType<SeedSummaries['query']>;
    perc(field:string|null,aggregate:SummaryAggregate,n:number): ReturnType<SeedSummaries['query']>;
    select(field:string|null,aggregate:SummaryAggregate): ReturnType<SeedSummaries['query']>;
    execute(): SummaryWindow & {span:number;count:number|bigint;series:SummarySeries[];percentiles:{field:string|null;aggregate:SummaryAggregate;n:number;value:unknown;samples:number;intervals:number}[]};
  };
  preview(from:number,to:number,where:SeedPredicate,context?:QueryContext): SummaryWindow & {
    count:{lower:number|bigint;upper:number|bigint}|null;
    coverageComplete:boolean;predicateComplete:boolean;fields:string[];pending:string[];
    eligible:[number,number][];periods:{time:number;lower:number|bigint;upper:number|bigint;complete:boolean}[];
  };
}
export interface LeafFileLayout {
  nodesEnd: number;
  nodeCount: number;
  blocks: {start:number; count:number; local:number; length:number}[];
}
export interface SeedLeaves extends SeedHandle {
  append(first: number, bytes: SeedInput): this;
}
export interface SeedReceiver extends SeedHandle {
  feed(bytes: BufferSource): void;
  strings(entries: [number,string][]): void;
  step(): {more:boolean;pack:SeedPack|null;missing?:number[]};
  load(bytes: BufferSource): SeedPack[];
  finish(): number;
}
export interface SeedBatch extends SeedHandle {
  origins(): {pack:number;row:number;definition:string}[];
  readonly length: number;
  readonly retainedBytes: number;
  objects(): SeedObject[];
  views(): Readonly<SeedObject>[];
}
export interface SeedQuery extends SeedHandle {
  context(value:QueryContext):this;
  /** Transfer an unstarted query into bounded Rust column growth. Yield the
   * host event loop between polls. No source values cross into JavaScript. */
  stalks(options?:{rows?:number}):SeedGrowth;
  filter(field: string, op: string, value: unknown): this;
  filter(predicate: SeedPredicate): this;
  cast(field:string,alias:string,cast:SeedCast):this;
  select(...fields: (string | string[])[]): this;
  /** Filter Pack Definition versions before traversing records. */
  version(major: number, minor: number): this;
  orderBy(field: string, direction?: 'asc' | 'desc'): this;
  limit(count: number): this;
  offset(count: number): this;
  fetch(count?: number): SeedBatch;
  /** Resumable execution. Yield the host event loop between calls. An empty
   * batch is not exhaustion; inspect done. Dispose each non-null batch. */
  stats(): {scanned:number;matched:number};
  poll(count?: number, work?: number): {batch: SeedBatch | null; done: boolean; work: number};
  objects(count?: number): SeedObject[];
  views(count?: number): Readonly<SeedObject>[];
  cancel(): void;
}
export interface SeedGrowth extends SeedHandle {
  poll(work?:number):{done:boolean;rows:number;bytes:number};
  finish():SeedStalks;
  cancel():void;
}
export interface SeedStalks extends SeedHandle {
  info():{rows:number;bytes:number;chunks:number};
  sum(field:string):number|bigint|null;
  /** Materialize only the selected page, at most 4096 rows. */
  objects(first?:number,count?:number):SeedObject[];
}
export interface SeedStore extends SeedHandle {
  pack(pack: SeedPack): this;
  query(): SeedQuery;
  reconstruct(length: number | bigint, count: number | bigint): SeedReplica;
}
/** A query scope bound by Rust to the selected trunk.trie registrations. */
export interface SeedReader extends SeedHandle {
  ready(owner:SeedReadiness,source:string):this;
  /** Retain complete metadata for at most count selected Packs. Repeat from
   * next until done, yielding between batches. Shared admission applies;
   * dispose the selection when it no longer needs metadata residency.
   * Compressed Packs must expand to extract metadata; member bytes are not pinned. */
  prepare(first?: number, count?: number): {next: number; done: boolean; bytes: number};
  /** Retain metadata from a collected Pack after these exact uncompressed bytes
   * are published at the selected address. Does not retain member payloads. */
  retain(ordinal: number, pack: SeedPack): this;
  /** Narrow time-ordered index rows to [from, to) before loading Pack payloads. */
  range(from: number | bigint, to: number | bigint): SeedReader;
  /** Intersect with eligible [from,to) periods. Rust seeks original row
   * ordinals using the shared index; no Packs or Definitions are copied. */
  ranges(periods: Iterable<readonly [number | bigint, number | bigint]>): SeedReader;
  /** Bind Time-selected hours and version-specific Leaves before reading.
   * Rust reads bounded index windows from the pinned host file. */
  index(file: number | bigint, publishedLength: number | bigint, time: SeedTimeline, hours: Iterable<number | bigint>, leaves?: Iterable<SeedLeaves>, space?: 'packs' | 'beds'): this;
  /** Combine this selection with additional file selections in one Rust cursor.
   * Ordering, offset and limit apply to the combined result. Host file leases
   * must remain pinned until its cursor and returned views are released. */
  query(additional?: Iterable<SeedReader | SeedPack>): SeedQuery;
  pack(ordinal: number): SeedPack;
}
export interface Packs extends SeedHandle {
  strings(entries: [number,string][]): void;
  /** On-demand Rust reads at Time-selected addresses. Keep the host file lease
   * and publication pinned until readers, cursors and result views are disposed.
   * Indexed result views can still read their retained index snapshot.
   * Readers of the same selection share resident Packs without copying. */
  reader(file: number | bigint, publishedLength: number | bigint, addresses: Iterable<number | bigint>, maximumPackBytes?: number): SeedReader;
  reconstruct(length: number | bigint, count: number | bigint): SeedReplica;
  /** Resolve each Pack's Definition reference within this registered scope. */
  receiveStored(maximumPackBytes: number): SeedReceiver;
  /** Check integrity before publication, then resolve each Pack through the trie. */
  receiveForWrite(maximumPackBytes: number): SeedReceiver;
  load(branch: number, bytes: SeedInput, maximumBytes?: number): this;
  /** Read selected Box children at the addresses supplied by the time trie. */
  loadBox(branch: number, bytes: SeedInput, addresses: Iterable<number | bigint>): this;
  /** Keep the prepared Pack and any index binding without reopening its bytes. */
  pack(branch: number, pack: SeedPack): this;
  query(): SeedQuery;
}
export type Beds = Packs;
export interface SeedBranches {
  /** Select matching resource branches across all registered versions. */
  select(branches: Iterable<number>): SeedBranches;
  /** All selected Definitions must have been prescribed from their .rx files. */
  packs(): Packs;
  /** Browsable storage shares the registered query scope on any node. */
  beds(): Beds;
  /** Read summary days through this branch's historical source selections. */
  summaries(): SeedSummaryPlan;
  /** Supply received hour ranges when reading the sparse replica Time file. */
  time(bytes: SeedInput, received?: Iterable<readonly [number,number]>): SeedTimeline;
  leaves(): Leaves;
}
export interface Leaves {
  labels(bytes: SeedInput): Record<number,string>;
  label(bytes: SeedInput, leaf: number, text: string | null): Uint8Array;
  read(bytes: SeedInput): SeedLeaves;
  /** Verify incoming integrity before a storage write. */
  inspect(bytes: SeedInput): LeafFileLayout;
  definitions(bytes: SeedInput | Uint8Array<ArrayBufferLike>): string[];
}
export interface SeedTimeline extends SeedHandle {
  retain():SeedTimeline;
  info(): {first:number;origin:number;tail:number|bigint;sealed:number;generation:number[];summary_offset:number;widths:number[]};
  get(ms:number|bigint): {hour:number;start:number|bigint;end:number|bigint;packs:number|bigint;beds:number|bigint;count:number|bigint}|null;
  /** Published hours intersecting [from, to); maximum 65,536 hours per call. */
  range(from:number|bigint,to:number|bigint): NonNullable<ReturnType<SeedTimeline['get']>>[];
}
/** Host observation durations are milliseconds; Rust stores microseconds. */
export type QueryEvent={time:number;id:number[];source:string;query:string;status:'complete'|'error'|'cancelled';error?:string;user_agent?:string;from:number;to:number;rows:number;duration:number;worker?:number;worker_start?:number;before_collection?:number;collection?:number;after_collection?:number;presentation?:number};
export interface SeedRuntime {
  readonly ports:{design(draft:unknown):unknown;sample(bytes:SeedInput,sourceBytes:number,profile?:unknown):unknown;review(mapping:unknown):{fields:{name:string;size:number;annotations:string[]}[]}};
  reads():SeedReadiness;
  stalks(query:SeedQuery,options?:{rows?:number}):SeedGrowth;
  queryEvent(event:QueryEvent):Uint8Array<ArrayBuffer>;
  readonly pack:{size(prefix:SeedInput):number|null;expand(bytes:SeedInput,maximumBytes?:number):Uint8Array};
  summaryReport(spec:{context?:QueryContext;from:number;to:number;step:number;metric?:string}):SummaryReport;
  summaryTable(spec:{context?:QueryContext;plan:unknown;from:number;to:number;coverage:{source:string;definition:string;time:number;count:number}[]}):SeedHandle & {
    read(reader:SeedSummaries,selection:{source:string;definition:string}):void;
    remaining():[string,string,[number,number][]][];
    events(batch:SeedBatch):void;
    result():{columns:string[];rows:unknown[][]};
  };
  summaryWindow(from:number,to:number): {requested:{from:number;to:number};from:number;to:number;period:number;periods:number};
  readControl(bytes: SeedInput | Uint8Array<ArrayBufferLike>): unknown;
  /** JSON-compatible discovery metadata, encoded by the Rust PIL codec. */
  writeControl(value: unknown): Uint8Array<ArrayBuffer>;
  trunk(bytes: BufferSource, options?: {writer?: SeedWriterOptions}): SeedTrunk;
  /** Read a Pack's encoded extent through the shared Rust PAC codec. */
  storedLength(prefix: SeedInput): number;
  /** Check received Pack integrity once before writing; opens do not check CRCs. */
  beforeWrite(bytes: BufferSource): void;
  prescribe(bytes: BufferSource, selection?: {namespace?: string;name: string;major: number;minor: number}): SeedDefinition;
  /** PACKS: Packed Array Container Kompressed Storage, available on any node. */
  packs(): SeedStore;
  packs(bytes: BufferSource, definitions: SeedDefinition[], addresses?: (number | bigint)[]): SeedStore;
  /** BEDS: Browsable Event Datastore, available on any node. */
  beds(): SeedStore;
  beds(bytes: BufferSource, definitions: SeedDefinition[], addresses?: (number | bigint)[]): SeedStore;
  summaries(bytes: BufferSource, first: number, end: number): SeedStore;
  readonly memoryBytes: number;
}
/** Open semantic mappings. Values remain canonical; groups are inclusive ranges. */
export interface SeedFieldMetadata {
  name: string;
  provider: string;
  field: string;
  kind: 'integer' | 'string';
  open: boolean;
  sources: string[];
  values: {value: number | string;label: string;safe?: boolean;idempotent?: boolean}[];
  groups: {min: number;max: number;label: string}[];
  views?: {name?:string;kind:'group'|'position'}[];
}
export interface SeedField {
  name: string;
  /** Annotation-derived view of this stored field; consumes no additional bytes. */
  source?: string;
  type: string;
  values: string[];
  annotations: string[];
  metadata?: SeedFieldMetadata;
}
export interface SeedTrunk extends SeedHandle {
  commits(): SeedCommits;
  trie(): SeedTrunk;
  branches(): SeedBranches;
  describe(id: number): {fields: SeedField[]};
  query<T=unknown>(request: unknown): T;
  section(id: number): Uint8Array;
  checkFile(file: string, bytes: Uint8Array): void;
  /** Uses exact registration identity/version; subsequent calls reuse RX. */
  prescribe(id: number, bytes?: BufferSource): SeedDefinition;
  prescribeFile(file: string, bytes: Uint8Array): void;
}
export interface SeedWriterOptions { memoryBytes: number; batches: number; operations: number }
export interface SeedReplica extends SeedHandle {
  retain():SeedReplica;
  /** Validate against committed Time; stage the returned byte patches in the
   * metadata transaction. This does not publish durable availability. */
  publication(time: SeedTimeline, generation: number[], placements: Iterable<NonNullable<ReturnType<SeedTimeline['get']>>>): [number,number][];
  /** Reuse the plan against current Time bytes; null requests a fresh plan. */
  publication(time: SeedTimeline, generation: number[], placements: Iterable<NonNullable<ReturnType<SeedTimeline['get']>>>, file: number): [number,number][] | null;
  batch(trunk: SeedTrunk, file: number | bigint, maximumOperations?: number): SeedWritePlan;
  /** Consume an ordered response against the pinned Time selection in Rust. */
  collect(time: SeedTimeline, ranges: Iterable<readonly [number,number]>, window: {generation:number[];retained:number;sealed:number}): SeedCollection;
}
export interface SeedCollection extends SeedHandle {
  peek():NonNullable<ReturnType<SeedTimeline['get']>>|null;
  /** Match Time before acquiring a file lease. Advances reception, not publication. */
  accept(pack: SeedPack): NonNullable<ReturnType<SeedTimeline['get']>>;
  /** Attach retained Pack bytes at Time's origin address. Failed admission can be retried. */
  load(batch: SeedWritePlan, pack: SeedPack, selection?: {reader:SeedReader;ordinal:number}): NonNullable<ReturnType<SeedTimeline['get']>>;
  /** Reject a truncated selection before publishing its coverage. */
  finish(): number;
}
export interface SeedWritePlan extends SeedHandle {
  /** Pack handles retain Rust memory directly; no host byte export/upload. */
  load(address: number | bigint, value: SeedInput | SeedPack): this;
  /** Write at the reader's selected address and retain complete metadata only
   * after commits.published(). Failed publication releases it during recovery. */
  load(reader: SeedReader, ordinal: number, pack: SeedPack): this;
  footer(): this;
  /** A full queue retains this plan; retry submit after backpressure clears. */
  submit(): bigint;
}
export type SeedCommitProgress = {state: 'idle'} | {state:'ready';id:bigint}
  | {state:'writing';id:bigint;calls:number;bytes:number};
export interface SeedCommits {
  /** Inspect ownership/backpressure without executing queued work. */
  pending(): number;
  /** Yield a host task between polls. Ready does not publish availability. */
  poll(maximumCalls?: number, maximumBytes?: number): SeedCommitProgress;
  /** Call only after the existing metadata publication succeeds. */
  published(id: number | bigint): void;
  failed(id: number | bigint): void;
  /** Call after restoring the published snapshot. All dependent plans drop. */
  recovered(): bigint[];
}
/** Retained synchronous environment handle. Methods only perform I/O; do not
 * retain the Rust memory view or reenter the runtime from these callbacks. */
export interface SeedFile {
  read?(bytes: Uint8Array, options: {at: number}): number;
  write(bytes: Uint8Array, options: {at: number}): number;
  truncate(length: number): void;
  flush(): void;
}
export function createSeeds(wasm?: URL | string | SeedBytes, options?: {files?: (id:number)=>SeedFile; residentBytes?: number}): Promise<SeedRuntime>;

export interface SummaryReport {read(reader:SeedSummaries,selection:{source:string;from:number;to:number;where?:SeedPredicate}):this;result():{summary:unknown;dashboard:unknown};dispose():void}
export interface SeedReadiness extends SeedHandle {
 queue(source:string,address:number|bigint):'queued'|'ready';
 admit(source:string,address:number|bigint,bytes:number,claim?:SeedCollection):boolean;
 load(source:string,address:number|bigint,pack:SeedPack,claim?:SeedCollection):void;
 state(source:string,address:number|bigint):'unknown'|'queued'|'ready';
 coverage(source:string,address:number|bigint,flags:number):number;
 published(source:string,address:number|bigint):void;
 submitted(source:string,address:number|bigint):void;
 discard(source:string,address:number|bigint,claim?:SeedCollection):void;
 cancel(source:string,claim?:SeedCollection):void;
 clear(source:string):void;
 used():number;
 pack(source:string,address:number|bigint):SeedPack;
}
