import assert from 'node:assert/strict';
import {readFile} from 'node:fs/promises';
import {createSeeds} from './seeds.mjs';
// summary_hour generates these through DOC builders and Rust Seed ingestion.
const prefix=process.argv[2];
if(!prefix)throw Error('summary_hour fixture prefix required');
const runtime=await createSeeds(await readFile(new URL('./data-seeds.wasm',import.meta.url)));
const definition=runtime.prescribe(await readFile(`${prefix}.source.rx`));
assert.deepEqual(definition.analytics(),[{ordinal:1,field:'duration',percentiles:[50,95]}]);
const start=Date.UTC(2026,0,1),from=start+60000,to=start+540000;
assert.deepEqual(runtime.summaryWindow(from,to),{requested:{from,to},from:start,to:start+600000,period:300000,periods:2});
assert.throws(()=>runtime.summaryWindow(to,from),/increase/);
assert.throws(()=>runtime.summaryWindow(NaN,to),/timestamps/);
const trunk=runtime.trunk(await readFile(`${prefix}.trunk`));
trunk.prescribeFile('summary-hour.rx',await readFile(`${prefix}.source.rx`));
trunk.prescribeFile('summary-hour-summaries.rx',await readFile(`${prefix}.summary.rx`));
assert.throws(()=>trunk.trie().branches().summaries(),/one registered branch/);
const prepared=trunk.trie().branches().select([1]).summaries();
const summaries=prepared.open(await readFile(`${prefix}.summ`),start);
const pack=prepared.pack(await readFile(`${prefix}.pac`),start);
const compressed=prepared.pack(await readFile(`${prefix}.pac.zstd`),start);
const registered=prepared.pack(await readFile(`${prefix}.pac`),start);
const registeredBox=prepared.open(await readFile(`${prefix}.summ`),start);
prepared.dispose();trunk.dispose(); // Readers retain the Rust plan and bytes.
definition.dispose(); // The reader retains the plan and its source Definition.
const result=summaries.query(from,to).perc('duration','mean',50).perc('duration','mean',95).perc('duration','mean',98).execute();
assert.deepEqual(result.requested,{from,to});assert.equal(result.from,start);assert.equal(result.to,start+600000);
assert.equal(result.count,5);
assert.deepEqual(pack.query(from,to).perc('duration','mean',50).perc('duration','mean',95).perc('duration','mean',98).execute(),result);
assert.deepEqual(compressed.query(from,to).perc('duration','mean',50).perc('duration','mean',95).perc('duration','mean',98).execute(),result);
assert.deepEqual(result.percentiles.map(p=>p.value),[150,225,225]);
for(const p of result.percentiles){assert.equal(p.samples,2);assert.equal(p.intervals,2);assert.equal(p.aggregate,'mean');}
const combined=summaries.query(from,to).span(600000).perc('duration','mean',95).execute();
assert.equal(combined.percentiles[0].value,180);assert.equal(combined.percentiles[0].samples,1);
const counts=summaries.query(from,to).perc(null,'count',50).execute();
assert.equal(counts.percentiles[0].value,2);
const chart=pack.query(from,to).select(null,'count').select('duration','mean').select('duration','mean').execute();
assert.deepEqual(chart.series,[
 {field:null,aggregate:'count',points:[{from:start,to:start+300000,value:3},{from:start+300000,to:start+600000,value:2}]},
 {field:'duration',aggregate:'mean',points:[{from:start,to:start+300000,value:150},{from:start+300000,to:start+600000,value:225}]},
]);
assert.deepEqual(chart.percentiles,[]);
assert.deepEqual(compressed.query(from,to).select(null,'count').select('duration','mean').execute(),chart);
assert.deepEqual(summaries.query(from,to).select(null,'count').select('duration','mean').execute(),chart);
assert.deepEqual(registered.query(from,to).select(null,'count').select('duration','mean').execute(),chart);
assert.deepEqual(registeredBox.query(from,to).select(null,'count').select('duration','mean').execute(),chart);
assert.throws(()=>summaries.query(from,to).span(1).execute(),/span/);
assert.throws(()=>summaries.query(from,to).perc('status','mean',95).execute(),/class/);
const extrema=summaries.query(from,to).perc('duration','mean',0).perc('duration','mean',100).execute();
assert.deepEqual(extrema.percentiles.map(p=>p.value),[150,225]);
const empty=summaries.preview(from,to,{field:'duration',op:'>',value:10000});
assert.deepEqual(empty.count,{lower:0,upper:0});assert.equal(empty.coverageComplete,true);assert.deepEqual(empty.eligible,[]);
assert.equal(empty.periods.length,2);assert.equal(empty.periods[0].time,start);
const unknown=summaries.preview(from,to,{field:'message',op:'=',value:'x'});
assert.equal(unknown.predicateComplete,false);assert.deepEqual(unknown.pending,['message']);assert.deepEqual(unknown.eligible,[[from,to]]);
const missing=summaries.preview(start-300000,to,{field:'duration',op:'>',value:10000});
assert.equal(missing.coverageComplete,false);assert.equal(missing.count,null);assert.deepEqual(missing.eligible,[[start-300000,start]]);
assert.throws(()=>summaries.query(start-1,to).execute(),/coverage/);
const unavailable=summaries.query(start+600000,start+900000).perc('duration','mean',95).execute();
assert.equal(unavailable.percentiles[0].value,null);
assert.throws(()=>summaries.query(from,to).perc('duration','mean',101).execute(),/percentile/);
pack.dispose();
registered.dispose();registeredBox.dispose();
compressed.dispose();
summaries.dispose();assert.throws(()=>summaries.query(from,to).execute(),/Released/);
console.log('Rust/WASM summary windows, rollups, query-time interval percentiles, coverage, pruning and ownership passed');
