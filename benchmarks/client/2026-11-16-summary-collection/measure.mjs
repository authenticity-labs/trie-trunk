// Existing fixtures were produced by Rust ingress; this benchmark only reads them.
import {performance} from 'node:perf_hooks';
import {day,start,bytes,snapshot,tap,digest} from '../../../client/services/generated-summary-fixture.mjs';
import {summaryReader} from '../../../client/services/pil/trunk.ts';
import {decodeSummaryDay} from '../../../client/services/pil/summaries.ts';
const raw=await bytes('pac'),reader=await day();
const cases={};
async function measure(name,run,n=100){
 for(let i=0;i<10;i++)await run();
 const samples=[];for(let i=0;i<7;i++){let t=performance.now();for(let j=0;j<n;j++)await run();samples.push((performance.now()-t)/n);}
 cases[name]={iterations:n,samplesMs:samples,medianMs:[...samples].sort((a,b)=>a-b)[3]};
}
await measure('openAndHourCounts',async()=>{const d=await decodeSummaryDay(raw,start,summaryReader(snapshot,tap,digest,raw,start));d.reader.dispose();});
for(const minutes of [5,60,1440])await measure('report'+minutes+'Minutes',()=>{
 const report=reader.runtime.summaryReport({from:start,to:start+minutes*60000,step:3600000,metric:'duration'});
 try{report.read(reader.reader,{source:tap,from:start,to:start+minutes*60000});const result=report.result();if(result.dashboard.totals.events<0)throw Error('Invalid report');}finally{report.dispose();}
});
reader.reader.dispose();console.log(JSON.stringify({bytes:raw.length,cases},null,2));
