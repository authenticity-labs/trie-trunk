import { readFile } from 'node:fs/promises';
import { performance } from 'node:perf_hooks';
import { createSeeds } from './seeds.mjs';
const [wasm,dir]=process.argv.slice(2);
const runtime=await createSeeds(await readFile(wasm));
const def=runtime.prescribe(await readFile(`${dir}/sample.rx`));
const pack=def.open(await readFile(`${dir}/sample.pac`));
const packs=runtime.packs().pack(pack);
const median=v=>[...v].sort((a,b)=>a-b)[v.length>>1];
let checksum=0;
function trial(mode,projection,access,readCount) {
  globalThis.seedBenchmarkResult=null;global.gc?.();
  const before=process.memoryUsage().heapUsed;
  const query=packs.query().select(projection);const t0=performance.now();const batch=query.fetch(10_000);const t1=performance.now();
  const rows=batch[mode]();const t2=performance.now();
  for(let i=0;i<Math.min(rows.length,readCount);i++)for(const name of access){const v=rows[i][name];checksum+=typeof v==='number'?v:v.length;}
  const t3=performance.now();
  // Keep the entire result observably reachable during GC; stack liveness can
  // otherwise make the measurement falsely report nearly zero/negative heap.
  globalThis.seedBenchmarkResult=rows;global.gc?.();
  const heap=process.memoryUsage().heapUsed-before;
  const result={timings:[t1-t0,t2-t1,t3-t2,t3-t0],heap,retainedPackBytes:mode==='views'?batch.retainedBytes:0,wasmMemoryBytes:runtime.memoryBytes};
  batch.dispose();query.dispose();return result;
}
const wide=Array.from({length:27},(_,i)=>`field${i+5}`);
const cases=[['one-column',['sequence'],['sequence'],10_000],['wide-first-page',wide,['field5'],100],['wide-all-fields',wide,wide,10_000]];
for(const [scenario,projection,access,count] of cases)for(const mode of ['views','objects']) {
  const samples=[];for(let i=0;i<7;i++){const value=trial(mode,projection,access,count);if(i>=2)samples.push(value);}
  console.log(JSON.stringify({scenario,mode,matched:10000,projectedColumns:projection.length,accessedColumns:access.length,accessedRows:count,
    samplesMs:samples.map(v=>v.timings),medianMs:{query:median(samples.map(v=>v.timings[0])),return:median(samples.map(v=>v.timings[1])),read:median(samples.map(v=>v.timings[2])),total:median(samples.map(v=>v.timings[3]))},
    jsHeapSamplesBytes:samples.map(v=>v.heap),medianJsHeapBytes:median(samples.map(v=>v.heap)),resultRetainsPackBytes:samples[0].retainedPackBytes,wasmLinearMemoryBytes:samples[0].wasmMemoryBytes}));
}
globalThis.seedBenchmarkResult=null;
console.log(JSON.stringify({checksum,node:process.version,platform:process.platform,arch:process.arch}));packs.dispose();pack.dispose();def.dispose();
