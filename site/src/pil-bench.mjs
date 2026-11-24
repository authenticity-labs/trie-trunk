import {readFileSync} from 'node:fs';
const read=name=>JSON.parse(readFileSync(new URL(`../bench/pil/${name}`,import.meta.url),'utf8'));
const compression=read('million-objects-zstd.json');
const fx=read('fx.json');
const api=read('api-performance.json');
const mb=bytes=>(bytes/1e6).toFixed(2)+' MB';
const percent=(bytes,base)=>(100*(1-bytes/base)).toFixed(1)+'%';
const bar=(label,bytes,base)=>`<div class="pil-bench-bar"><div><span>${label}</span><strong>${mb(bytes)}</strong></div><div class="pil-bench-track"><span style="width:${100*bytes/base}%"></span></div></div>`;
export function pilBenchPanels(table){
 const {totals}=compression;
 const sample=fx.cases.find(row=>row.fields===32);
 const op=name=>sample.operations.find(row=>row.operation===name).median_ns.toFixed(1)+' ns';
 const browser=api.browser.foregroundRebuiltWasm;
 const browserMs=name=>browser.results.find(row=>row.name===name).afterMs.toFixed(1)+' ms';
 return [
 ['comparison','Comparison',`<h3>One Million Objects</h3>
 <div class="pil-bench-highlight"><strong>${percent(totals.pil_zstd1,totals.json)} Smaller</strong><span>Pill Packs with Zstd-1 compared with raw JSON</span></div>
 ${bar('Raw JSON',totals.json,totals.json)}
 ${bar('Pill Packs',totals.pil,totals.json)}
 ${bar('Pill Packs · Zstd-1',totals.pil_zstd1,totals.json)}
 ${table(['Footprint','JSON','PIL','Protobuf','FlatBuffers','FIX'],[
 ['Uncompressed',mb(totals.json),mb(totals.pil),'Not Measured','Not Measured','Not Measured'],
 ['Zstd-1',mb(totals.json_zstd1),mb(totals.pil_zstd1),'Not Measured','Not Measured','Not Measured']])}
 <p>PIL is ${percent(totals.pil,totals.json)} smaller before compression. With Zstd-1 on both formats, its packs are ${percent(totals.pil_zstd1,totals.json_zstd1)} smaller than JSON.</p>
 <details class="pil-detail"><summary>Dataset and Measurement</summary><p>${compression.date} · One million deterministic synthetic orders: twelve fields with varied numbers, text, dictionaries, absent values, and nulls. Both formats use the same 1,000 groups of 1,000 objects. JSON is compact JSONL; PIL includes pack framing and one shared ${compression.definition_bytes}-byte Definition.</p><p>Every Pill was decoded and compared with its source object. Every compression batch was decompressed and checked byte-for-byte. JSON uses Node’s Zstd ${compression.compression.json.split('Zstd ')[1]}; PIL uses the existing Rust/WASM PAC sealer, both at level 1. These are footprint measurements, not speed measurements. Other formats await equivalent measurements.</p><a class="text-link" href="/pill-bench/million-objects-zstd.json">Source Measurements</a> · <a class="text-link" href="/pill-bench/measure-million.mjs">Reproduce This Dataset</a></details>`],
 ['compression','Compression',`<h3>Compression on Both Sides</h3><p>One million identical objects and identical grouping. Zstd-1 is applied to both formats.</p>
 ${table(['Footprint','JSON','PIL Packs'],[
 ['Uncompressed',mb(totals.json),mb(totals.pil)],
 ['Zstd-1',mb(totals.json_zstd1),mb(totals.pil_zstd1)],
 ['Reduction from Raw JSON',percent(totals.json_zstd1,totals.json),percent(totals.pil_zstd1,totals.json)]])}
 <p>JSON compresses repeated field names well. PIL starts smaller and retains a ${percent(totals.pil_zstd1,totals.json_zstd1)} size advantage after compression on this dataset.</p>
 <details class="pil-detail"><summary>Measurement Scope</summary><p>Each batch is compressed independently. PIL totals include PAC framing and one shared Definition. Payload values are preserved in full; no external index is required. Compression ratios depend on the data and pack size.</p><a class="text-link" href="/pill-bench/million-objects-zstd.json">Source Measurements</a></details>`],
 ['datasets','Datasets',`<h3>Working with Many Objects</h3>
 <p>RX is the everyday codec for objects whose values and populated fields vary. FX is the fast path when you can guarantee a fixed layout: register it once, then read each field at a known offset. Neither requires expanding the whole object just to read one field.</p>
 ${table(['Dataset Choice','JSON','PIL · RX','PIL · FX'],[
 ['Useful For','Public APIs, configuration, and consumers that already speak JSON','Transactional API objects, documents, and heterogeneous datasets sharing a Definition','Dense numeric tables, sensor samples, and fixed-layout messages read repeatedly'],
 ['Read a Field','Parse text, then access the object','Locate the selected field using the Pill’s field states and lengths','Use the offset prepared at registration'],
 ['Optional Values','Omit the property or store null','Absent values occupy no value bytes','Absent values retain their slots; accessors check presence'],
 ['Variable-Length Values','Supported','Supported','Requires a fixed-capacity layout'],
 ['Tradeoff','Readable and widely supported; repeats names and needs text parsing','Compact varied objects; field locations can vary between Pills','Predictable access; reserved capacity costs bytes even when unused']])}
 <p>For a customer object with optional addresses and notes, choose RX. For a dense stream of fixed-size coordinates read many times, FX can avoid repeated layout traversal. Keep JSON where human readability or an existing consumer matters most. Accessing an already-parsed JavaScript object is a different operation from parsing JSON or reading encoded Pills.</p>
 <h3>Smaller Objects, Smaller Datasets</h3>
 <p>On the million-order dataset, Pills store values against one shared Definition instead of repeating field names. Packs group those Pills for addressing and compression.</p>
 ${table(['Footprint','JSON','Pills · RX','Pill Packs · RX'],[
 ['Uncompressed',mb(totals.json),mb(totals.pills),mb(totals.pil)],
 ['Zstd-1',mb(totals.json_zstd1),'Not Measured Separately',mb(totals.pil_zstd1)]])}
 <p>That is ${percent(totals.pills,totals.json)} less space for Pills before compression, and ${percent(totals.pil_zstd1,totals.json)} less for compressed packs versus raw JSON. Against compressed JSON, the packs save ${percent(totals.pil_zstd1,totals.json_zstd1)}. This variable-text dataset was not encoded with FX; fixed capacities would need to be chosen before a fair comparison.</p>
 <details class="pil-detail"><summary>Footprint Method</summary><p>${compression.date} · One million synthetic orders, twelve fields, identical values in each format. PIL totals include one shared Definition. Packs include their headers and member directories. Every object and compressed batch was verified. This run does not measure encoding or decoding speed.</p><a class="text-link" href="/pill-bench/million-objects-zstd.json">Source Measurements</a></details>
 <h3>100,000 Objects in the Browser</h3>
 ${table(['Operation','JSON','PIL · RX','PIL · FX'],[
 ['Pack All Objects','Not Measured',browserMs('100k records: batch pack'),'Not Measured'],
 ['Open and Validate','Not Measured',browserMs('100k records: full open'),'Not Measured'],
 ['Read One Field per Object','Not Measured',browserMs('100k records: get + field'),'Not Measured']])}
 <details class="pil-detail"><summary>Browser Measurement</summary><p>October 30, 2026 · JavaScript with WASM codecs · Chrome 156 · Apple M1 Max. Median of five samples after two warmups; 100,000 Pills in a 10,406,553-byte PAC. Codec work only, excluding network, disk, and rendering. JSON and fixed-layout measurements on this dataset have not been run.</p><a class="text-link" href="/pill-bench/api-performance.json">Source Measurements</a></details>
 <h3>Repeated Reads and Writes</h3><p>A separate native measurement uses 1,024 varying objects with 32 fields. Both Pill layouts occupy 264 bytes per object in this all-present dataset.</p>
 ${table(['Per-Object Operation','JSON','PIL · RX','PIL · FX'],[
 ['Encode with a Reused Buffer','Not Measured',op('pil/encode-reused-buffer'),op('fx/encode-reused-buffer')],
 ['Validate and Read the Last Field','Not Measured',op('pil/reused-validated-index-and-last-field'),op('fx/validate-and-last-field')],
 ['Prepare and Read the Last Field','Not Measured',op('pil/reused-structural-index-and-last-field'),op('fx/fill-and-last-field')]])}
 <details class="pil-detail"><summary>Native Measurement</summary><p>November 5, 2026 · Rust release build · Apple M1 Max. Median of three runs of nine timed batches. The measured RX path reuses its encoder and index; both paths allocate zero bytes per measured operation. Definition verification happens once at registration.</p><p>The 18.5× access comparison measures RX building a structural index and reading the last field versus FX checking the record extent and reading at its precomputed offset. FX relies on the application’s fixed-layout guarantee. Full validation is measured separately. These timings do not include JSON conversion or table rendering and are not extrapolations of the browser dataset.</p><p>Fixed-layout Pills require FX decoding, currently available in Rust and the command line.</p><a class="text-link" href="/pill-bench/fx.json">Source Measurements</a></details>`]
 ];
}
