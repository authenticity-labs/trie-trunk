import {readFileSync} from 'node:fs';
import {performance} from 'node:perf_hooks';
import assert from 'node:assert/strict';
import {createSeeds} from '../../../rust/seeds/js/seeds.mjs';
const [beforePath,afterPath,fixtures]=process.argv.slice(2);
const runtimes=await Promise.all([beforePath,afterPath].map(p=>createSeeds(readFileSync(p))));
const output={description:'288 occupied five-minute periods; 10 Rust-ingested events per period; same bytes for both runtimes',cases:[]};
async function timing(run,n=30){for(let i=0;i<5;i++)run();const samples=[];for(let i=0;i<7;i++){const t=performance.now();for(let j=0;j<n;j++)run();samples.push((performance.now()-t)/n);}return {iterations:n,samplesMs:samples,medianMs:[...samples].sort((a,b)=>a-b)[3]};}
for(const width of [1,8,32]){
 const rx=readFileSync(`${fixtures}/${width}.rx`),raw=readFileSync(`${fixtures}/${width}.summ`);
 const definitions=runtimes.map(r=>r.prescribe(rx)),readers=definitions.map(d=>d.summaries(raw,0));
 try{for(const minutes of [5,1440])for(const filtered of [false,true]){
  const run=index=>{const report=runtimes[index].summaryReport({from:0,to:minutes*60000,step:3600000,metric:'duration'});try{report.read(readers[index],{source:'demo/bench/events/sample',from:0,to:minutes*60000,...(filtered?{where:{field:'duration',op:'>=',value:100}}:{})});return report.result();}finally{report.dispose();}};
  assert.deepEqual(run(1),run(0),'optimized report must match baseline exactly');
  const before=await timing(()=>run(0)),after=await timing(()=>run(1));
  output.cases.push({width,minutes,filtered,bytes:raw.length,before,after});
 }}finally{readers.forEach(r=>r.dispose());definitions.forEach(d=>d.dispose());}
}
console.log(JSON.stringify(output,null,2));
