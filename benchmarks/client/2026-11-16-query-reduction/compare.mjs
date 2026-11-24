// Existing dense fixtures are produced through Rust Seed ingestion/accumulators.
import {readFileSync} from 'node:fs';
import {performance} from 'node:perf_hooks';
import assert from 'node:assert/strict';
import {createSeeds} from '../../../rust/seeds/js/seeds.mjs';
const [beforePath,afterPath,queryPath,fixtures]=process.argv.slice(2);
const runtimes=await Promise.all([beforePath,afterPath].map(p=>createSeeds(readFileSync(p))));
const api=(await WebAssembly.instantiate(readFileSync(queryPath),{})).instance.exports;
const encoder=new TextEncoder(),decoder=new TextDecoder();
function reduce(plan,rows){const bytes=encoder.encode(JSON.stringify({op:'reduce',plan,rows})),p=api.input(bytes.length);new Uint8Array(api.memory.buffer,p,bytes.length).set(bytes);api.execute();const result=JSON.parse(decoder.decode(new Uint8Array(api.memory.buffer,api.output(),api.output_len())));if(result.error)throw Error(result.error);return result.value;}
function integer(v){return typeof v==='bigint'?{$integer:String(v)}:Array.isArray(v)?v.map(integer):v&&typeof v==='object'?Object.fromEntries(Object.entries(v).map(([k,v])=>[k,integer(v)])):v;}
const output={platform:process.platform,arch:process.arch,node:process.version,cases:[]};
const median=v=>[...v].sort((a,b)=>a-b)[v.length>>1];
for(const width of [1,8,32]){
 const raw=readFileSync(`${fixtures}/${width}.summ`),rx=readFileSync(`${fixtures}/${width}.rx`);
 const defs=runtimes.map(r=>r.prescribe(rx)),readers=defs.map(d=>d.summaries(raw,0));
 try {for(const field of ['count','duration.sum']){
  const plan={source:'summaries',filters:[],relational:{selection:[{field,function:'sum',alias:'total'}],groupBy:[]}};
  const coverage=readers[0].info().hours.map(h=>({source:'bench',definition:'rx',time:h.time,count:h.count}));
  const before=()=>{
   // Former route: decode every summary class into JS, flatten, serialize,
   // upload to the query WASM, reduce and materialize the final result.
   const rows=readers[0].rows(0,86400000).map(row=>{
    const values={bucket:row.time,count:integer(row.count)};
    for(const [name,fields] of Object.entries(row.fields))for(const [key,v] of Object.entries(fields??{}))values[`${name}.${key}`]=integer(v);
    return {count:row.count,values};
   });return reduce(plan,rows);
  };
  const after=()=>{const table=runtimes[1].summaryTable({plan,from:0,to:86400000,coverage});try{table.read(readers[1],{source:'bench',definition:'rx'});return table.result();}finally{table.dispose();}};
  assert.deepEqual(after(),before());
  const times=[[],[]],runs=[before,after];for(let n=0;n<12;n++)for(const i of n%2?[1,0]:[0,1]){const t=performance.now();for(let j=0;j<20;j++)runs[i]();if(n>=3)times[i].push((performance.now()-t)/20);}
  output.cases.push({width,field,bytes:raw.length,before:{medianMs:median(times[0]),samples:times[0]},after:{medianMs:median(times[1]),samples:times[1]}});
 }}finally{readers.forEach(r=>r.dispose());defs.forEach(d=>d.dispose());}
}
console.log(JSON.stringify(output,null,2));
