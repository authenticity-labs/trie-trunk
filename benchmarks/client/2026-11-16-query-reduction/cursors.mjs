import {readFileSync} from 'node:fs';
import {performance} from 'node:perf_hooks';
import assert from 'node:assert/strict';
import {createSeeds} from '../../../rust/seeds/js/seeds.mjs';
const [beforePath,afterPath,dir]=process.argv.slice(2);
const runtimes=await Promise.all([beforePath,afterPath].map(p=>createSeeds(readFileSync(p))));
const defs=runtimes.map(r=>r.prescribe(readFileSync(`${dir}/sample.rx`))),packs=defs.map(d=>d.open(readFileSync(`${dir}/sample.pac`))),beds=runtimes.map((r,i)=>r.beds().pack(packs[i]));
const result={platform:process.platform,cases:[]};
const median=v=>[...v].sort((a,b)=>a-b)[v.length>>1];
for(const mode of ['one','four','changing','stalk']){
 let ordinal=0;
 const run=i=>{
  const q=beds[i].query().select(...(mode==='one'?['sequence']:['sequence','reading','note','active'])).filter('sequence','>=',mode==='changing'?ordinal++%16:95).limit(mode==='stalk'?10000:100);
  if(mode!=='stalk'){try {const rows=q.objects(100);assert.equal(rows.length,100);return rows;}finally{q.dispose();}}
  const growth=q.stalks({rows:10000});try{while(!growth.poll(1024).done){}const stalk=growth.finish();try{return {rows:stalk.info().rows,bytes:stalk.info().bytes,sum:stalk.sum('sequence')};}finally{stalk.dispose();}}finally{growth.dispose();}
 };
 if(mode!=='changing')assert.deepEqual(run(0),mode==='stalk'?{...run(1),bytes:run(0).bytes}:run(1));
 const times=[[],[]];let outputs=[];for(let n=0;n<12;n++)for(const i of n%2?[1,0]:[0,1]){const t=performance.now();for(let k=0;k<(mode==='stalk'?5:100);k++)outputs[i]=run(i);if(n>=3)times[i].push((performance.now()-t)/(mode==='stalk'?5:100));}
 result.cases.push({mode,before:{medianMs:median(times[0]),samples:times[0]},after:{medianMs:median(times[1]),samples:times[1]},...(mode==='stalk'?{beforeResult:outputs[0],afterResult:outputs[1]}:{})});
}
beds.forEach(v=>v.dispose());packs.forEach(v=>v.dispose());defs.forEach(v=>v.dispose());console.log(JSON.stringify(result,(_k,v)=>typeof v==='bigint'?v.toString():v,2));
