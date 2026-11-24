import json,statistics,collections,hashlib
from pathlib import Path
out=Path(__file__).parent
rows=[json.loads(l) for l in (out/'results.jsonl').read_text().splitlines()]
g=collections.defaultdict(list)
for r in rows:g[r['dataset'],r['codec'],r['level'],r['operation']].append(r)
assert len(rows)==198 and all(len(v)==3 for v in g.values())
med=lambda xs,key:statistics.median(x[key] for x in xs)
lines=['# PAC Codec Comparison — 2026-11-02','',
'Isolated native Rust comparison. No live codec, format, storage path, or application dependencies changed.','',
'## Method','',
'- Rust gzip calls Pill Packer’s existing `compression::deflate` / `inflate`, using flate2 1.1.10 with its Rust backend. Brotli uses pure-Rust brotli 8.0.2. Zstandard uses the Rust zstd 0.13.3 crate backed by bundled C libzstd 1.5.7; this is not a pure-Rust Zstandard encoder.',
'- Same raw PAC bytes and independent pack boundaries for every codec. No dictionary training. Each pack gets a fresh codec context; an output buffer is reused, matching the current Rust gzip API. Brotli default window settings, requested quality; Zstandard default streaming settings and requested level, no worker threads.',
'- Three fresh processes per case, randomized case order. One untimed warm-up and byte-for-byte round-trip verification. Each process measures at least 200 ms of repeated passes. Tables report medians; raw per-process results retained.',
'- Wall time and process CPU time cover compression/decompression only, including codec creation and output writes to memory. They exclude PAC assembly, index work, file reads/writes, network, UI rendering, and WASM/JS crossings.',
'- Memory is peak resident process size (macOS ru_maxrss), including loaded input/output fixtures, runtime, buffers and codec working memory. It is NOT codec-only allocation or a browser-memory estimate. Encoding and decoding run in separate processes, so encoding peaks do not contaminate decode peaks.',
'- Stored size includes a common 16-byte compressed-PAC envelope per pack. The existing .pods file header and all companion files are unchanged and excluded. This does not implement codec identifiers for Brotli or Zstandard.',
'- All codecs recover identical bytes. Decoded PAC size is invariant; changing compression alone cannot reduce decoded .sods content. Native decode here is a proxy for the decoding step, not a measurement of the actual browser .sods path.',
'', '## Datasets','',
'Observed: deterministic 128-pack sample plus every PAC >=4 KiB from the current sandbox compressed branches. WRITE PACs are deliberately excluded because application policy leaves them uncompressed. Corpus provenance and SHA-256 hashes are in corpus.json.',
'',
'Dense and Sparse: 10,000 synthetic records each from the existing pacWorkload fixture, passed through the actual Store.AppendBatch → Flush → writeSealedHour → packBlock path in a disposable store. The existing server removes index-backed values and builds the actual hourly PAC. The extracted decoded PAC is the identical input to each codec. Sparse varies populated fields across records. These controlled synthetic workloads are not a claim about production compressibility.','']
cases=[('gzip',1),('gzip',6),('gzip',9),('zstd',1),('zstd',3),('zstd',9),('zstd',19),('br',1),('br',5),('br',9),('br',11)]
summary=[]
for ds in ['observed','dense','sparse']:
 sample=g[ds,'gzip',1,'encode'][0]
 lines += [f'## {ds.title()}: {sample["packs"]} PACs, {sample["records"]:,} Records', '',f'Decoded PAC bytes: {sample["raw_bytes"]:,}. Times below are for one pass over this entire dataset.','',
 '| Codec / Level | Stored Bytes | Bytes / Record | Encode Wall / CPU ms | Decode Wall / CPU ms | Encode / Decode Peak RSS MiB |',
 '|---|---:|---:|---:|---:|---:|']
 for codec,level in cases:
  a,b=g[ds,codec,level,'encode'],g[ds,codec,level,'decode'];r=a[0]
  row=dict(dataset=ds,codec=codec,level=level,raw_bytes=r['raw_bytes'],stored_bytes=r['stored_bytes'],records=r['records'],encode_wall_ms=med(a,'wall_ms'),encode_cpu_ms=med(a,'cpu_ms'),decode_wall_ms=med(b,'wall_ms'),decode_cpu_ms=med(b,'cpu_ms'),encode_peak_rss_bytes=med(a,'peak_rss_bytes'),decode_peak_rss_bytes=med(b,'peak_rss_bytes'))
  summary.append(row)
  lines.append(f'| {codec} {level} | {r["stored_bytes"]:,} | {r["stored_bytes"]/r["records"]:.2f} | {med(a,"wall_ms"):.3f} / {med(a,"cpu_ms"):.3f} | {med(b,"wall_ms"):.3f} / {med(b,"cpu_ms"):.3f} | {med(a,"peak_rss_bytes")/1048576:.2f} / {med(b,"peak_rss_bytes")/1048576:.2f} |')
 lines.append('')
lines += ['## Current Go Baseline and Allocation Boundary','',
'The deployed server path is Go `writeSealedHour` → `packBlock` → `pill-packer/go/codec.CompressPAC`. The latter creates a gzip writer and a bytes.Buffer, then copies the compressed body into the PAC output. The Rust ingress arena optimization does not eliminate these sealing allocations. Native ingress buffer adoption is distinct from sealing.','',
'The separate baseline calls the existing Go CompressPAC/ExpandPAC functions on the same PACs. Its allocation column is cumulative Go heap allocation per dataset pass, NOT simultaneous resident memory. These API calls also include envelope validation/construction and do not reuse output buffers as the Rust codec harness does, so this is a deployed-path baseline rather than a language-only comparison.','',
'| Dataset | Gzip Level | Stored Bytes | Encode Wall / CPU ms | Decode Wall / CPU ms | Encode / Decode Go Allocations MiB per Pass | Encode / Decode Peak RSS MiB |',
'|---|---:|---:|---:|---:|---:|---:|']
go=[json.loads(l) for l in (out/'go-results.jsonl').read_text().splitlines()];gg=collections.defaultdict(list)
for r in go:gg[r['dataset'],r['level'],r['operation']].append(r)
assert len(go)==36
for ds in ['observed','dense','sparse']:
 for level in [1,6]:
  a,b=gg[ds,level,'encode'],gg[ds,level,'decode']
  lines.append(f'| {ds} | {level} | {a[0]["stored_bytes"]:,} | {med(a,"wall_ms"):.3f} / {med(a,"cpu_ms"):.3f} | {med(b,"wall_ms"):.3f} / {med(b,"cpu_ms"):.3f} | {med(a,"allocated_bytes_per_pass")/1048576:.2f} / {med(b,"allocated_bytes_per_pass")/1048576:.2f} | {med(a,"peak_rss_bytes")/1048576:.2f} / {med(b,"peak_rss_bytes")/1048576:.2f} |')
lines+=['','## Reproduction','',
'1. Run extract.py from the repository root. It snapshots existing .pods files read-only and writes benchmark PACs under /tmp/trunk-pac-codec-compare/data/observed.',
'2. From go/, run `PAC_CODEC_FIXTURES=/tmp/trunk-pac-codec-compare/data go test ./server/cache -run ^TestExportCompressionComparisonPACs$ -count=1 -v`.',
'3. Build the saved Cargo.toml with `cargo build --release --manifest-path benchmarks/storage/2026-11-02-pac-codecs/Cargo.toml --target-dir /tmp/trunk-pac-codec-compare/rust/target`. It references the local pill-packer checkout. Cargo.lock pins codec dependencies.',
'4. Run run.py from the repository root. For the Go baseline, compile go-baseline.go in a temporary module requiring the local pill-packer/go module, place executable at /tmp/trunk-pac-codec-compare/go-baseline, then run run-go.py.',
'5. Run report.py. Fixture bytes and build products stay outside the repository. Reports, corpus fingerprints, benchmark source and lockfile are retained.','']
(out/'report.md').write_text('\n'.join(lines));(out/'summary.json').write_text(json.dumps(summary,indent=2)+'\n')
for ds in ['dense','sparse']:
 p=Path('/tmp/trunk-pac-codec-compare/data')/ds/'10000.pac';raw=p.read_bytes()
 (out/(ds+'-fixture.json')).write_text(json.dumps({'bytes':len(raw),'sha256':hashlib.sha256(raw).hexdigest()},indent=2)+'\n')
print('Report written')
