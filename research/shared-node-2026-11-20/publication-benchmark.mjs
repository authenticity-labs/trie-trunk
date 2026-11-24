// Run from the repository with --import ./research/benchmarks/query-regression-2026-11-18/types.mjs.
// Same registered snapshot and placements; preparation + detached Time update,
// excluding disk I/O and Pack encoding. Not an end-to-end query benchmark.
import {setup} from '../../client/services/registered-range-fixture.mjs';
import {markTimeFileRanges,timeLeaf} from '../../client/services/store/time-trie.ts';
import {SeedFiles} from '../../client/services/store/seed-writer.ts';
const files=new SeedFiles(),f=await setup({files:files.get}),generation=new Uint8Array(f.info.generation);
const ready=markTimeFileRanges(f.received,generation,[{from:f.info.first,to:f.info.sealed,files:6}]);
const ranges=ready.ranges.map(r=>[r.from,r.to]);
const time=f.branches.time(ready.bytes,ranges);
try {
 await f.collect(f.reply(),async({replica,members})=>{
  const placements=members.map(m=>m.placement);
  const implementations={
   previous(){
    const completed=placements.map(p=>{
     const leaf=timeLeaf(ready,p.hour);
     if(!leaf||BigInt(leaf.beds)!==BigInt(p.beds)||BigInt(leaf.count)!==BigInt(p.count)||(leaf.flags&6)!==6)throw Error('Invalid placement');
     return {from:p.hour,to:p.hour+3600000,files:16};
    });
    return markTimeFileRanges(ready,generation,completed).bytes;
   },
   rust(){
    const readonly=()=>{throw Error('read only');};
    const handle=files.bind({getSize:()=>ready.bytes.length,read:(out,{at})=>{const bytes=ready.bytes.subarray(at,at+out.length);out.set(bytes);return bytes.length},write:readonly,truncate:readonly,flush:readonly,close(){}});
    try {
     const patches=replica.publication(time,f.info.generation,placements,handle.id),bytes=ready.bytes.slice();
     if(patches===null)throw Error('Unexpected plan change');
     for(const [at,value] of patches)bytes[at]=value;
     return bytes;
    }finally{handle.close();}
   },
  };
  const samples={previous:[],rust:[]};let checksum=0;
  for(let iteration=0;iteration<550;iteration++)for(const name of iteration%2?['rust','previous']:['previous','rust']){
   const start=performance.now(),result=implementations[name]();checksum+=result.length;
   if(iteration>=50)samples[name].push(performance.now()-start);
  }
  const stats=values=>{values.sort((a,b)=>a-b);return {median_ms:values[values.length>>1],p95_ms:values[Math.floor(values.length*.95)]}};
  console.log(JSON.stringify({snapshot_bytes:ready.bytes.length,packs:placements.length,samples:500,previous:stats(samples.previous),rust:stats(samples.rust),checksum},null,2));
 });
}finally{time.dispose();f.close();}
