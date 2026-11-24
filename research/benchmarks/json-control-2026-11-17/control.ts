// Benchmark only: export existing Rust-decoded events, then compare a JSON API.
// This is deliberately NOT a second production query engine or codec.
const encode=(_key:string,v:any):any=>typeof v==='bigint'?{$bigint:String(v)}:
 v instanceof Uint8Array?{$bytes:Array.from(v)}:v instanceof ArrayBuffer?{$bytes:Array.from(new Uint8Array(v))}:v;
const decode=(_key:string,v:any):any=>v?.$bigint!==undefined?BigInt(v.$bigint):v?.$bytes?new Uint8Array(v.$bytes):v;
// Resolve test configuration at module startup, before the user submits. It is
// constant for this isolated app process; do not add an API round trip per query.
const configured=fetch('/caches?__bench=mode',{cache:'no-store'}).then(async r=>{
 if(!r.ok)throw Error('Benchmark mode unavailable');return r.text();
});
const digest=(objects:any[])=>{
 let hash=2166136261;
 // Compare actual values and Definition identity; snapshot-dependent keys differ.
 const canonical=(v:any):any=>typeof v==='bigint'?{$bigint:String(v)}:v instanceof Uint8Array?{$bytes:[...v]}:
  Array.isArray(v)?v.map(canonical):v&&typeof v==='object'?Object.fromEntries(Object.keys(v).sort().map(k=>[k,canonical(v[k])])):v;
 const text=JSON.stringify(objects.map(o=>canonical({definition:o.definition,values:o.values})));
 for(let i=0;i<text.length;i++)hash=Math.imul(hash^text.charCodeAt(i),16777619);
 return hash>>>0;
};
export async function control(original:any,args:any[]){
 const [trunk,query,token,signal,options,calendar,onPlan,boundary]=args;
 const trace=boundary?.trace,stamp=performance.now();
 const mode=await configured;
 if(trace)trace.times['control.mode']=performance.now()-stamp;
 if(mode==='binary'){
  const result=await original(...args);
  if(trace)trace.counts.pageHash=digest(result.objects);
  return result;
 }
 if(mode==='export'){
  await (await options.fetcher('/caches?__bench=progress&stage=export-start')).text();
  // No generated records: read all existing events using the actual Rust reader.
  const all=await original(trunk,{...query,size:20000,query:{filters:[],order:'asc'}},token,signal,options,calendar,onPlan,boundary);
  await (await options.fetcher('/caches?__bench=progress&stage=export-read')).text();
  if(all.objects.length!==20000||all.result.next)throw Error('Export is incomplete');
  const groups=new Map<string,any>();
  for(const o of all.objects){
   const key=JSON.stringify([o.definition,o.stream,o.branchId]);
   if(!groups.has(key))groups.set(key,{definition:o.definition,stream:o.stream,branchId:o.branchId,rows:[]});
   groups.get(key).rows.push(o.values);
  }
  const body=JSON.stringify({query,view:{...all,objects:[],result:{...all.result,page:[]}},groups:[...groups.values()]},encode);
  await (await options.fetcher('/caches?__bench=progress&stage=export-json&bytes='+body.length)).text();
  const saved=await options.fetcher('/caches?__bench=export',{method:'POST',body,signal});
  if(!saved.ok)throw Error('Export failed');
  all.objects=all.objects.slice(0,100);all.result.page=all.result.page.slice(0,100);
  return all;
 }
 if(mode!=='json'&&mode!=='json-http')throw Error('Unknown benchmark mode');
 const started=performance.now();
 // Keep actual preview/summary refinement and the same worker/UI delivery.
 await onPlan?.(structuredClone(calendar));signal.throwIfAborted();
 if(trace)trace.times['json.summary']=performance.now()-started;
 if(boundary)boundary.replicationStarted=performance.now();
 let at=performance.now();
 const get=mode==='json-http'?fetch:options.fetcher;
 const data=await get('/caches?__bench=events',{signal,cache:'no-store'});
 if(!data.ok)throw Error('JSON API failed');
 if(trace)trace.times['json.response']=performance.now()-at;
 at=performance.now();const text=await data.text();
 if(trace)trace.times['json.body']=performance.now()-at;
 at=performance.now();const saved=JSON.parse(text);
 saved.view.definitions=JSON.parse(JSON.stringify(saved.view.definitions),decode);
 if(trace)trace.times['json.parse']=performance.now()-at;
 if(boundary)boundary.replicationEnded=performance.now();
 // Only this one explicit workload is supported. Fail rather than compare a
 // different predicate, time unit, ordering, resource, or pagination silently.
 if(query.tap!==saved.query.tap||query.from!==saved.query.from||query.to!==saved.query.to||
    JSON.stringify(query.query)!==JSON.stringify(saved.query.query)||query.after)
  throw Error('JSON control only supports the exported first-page query');
 const plan=query.query;
 const filter=plan.filters?.[0]??(plan.predicate?.kind==='filter'?plan.predicate.filter:undefined);
 if(!filter||filter.field!=='duration'||filter.op!=='>='||filter.value.kind!=='number'||filter.value.value!=='10000'||plan.order!=='asc'||plan.search||plan.relational)
  throw Error('Unexpected JSON control predicate: '+JSON.stringify(plan));
 at=performance.now();const view=saved.view;
 const definition=view.definitions[0].registered;
 const time=definition.fields.find((f:any)=>f.annotations.includes('@time'))?.name;
 if(!time)throw Error('Expected millisecond time field');
 const from=BigInt(Date.parse(query.from)),to=BigInt(Date.parse(query.to));
 const matches:any[]=[];let scanned=0;
 for(const group of saved.groups)for(const values of group.rows){
  const ordinal=scanned++;
  if(BigInt(values[time])>=from&&BigInt(values[time])<to&&values.duration>=10000)
   matches.push({values,definition:group.definition,stream:group.stream,branchId:group.branchId,key:`${view.result.snapshot}:${ordinal}`});
 }
 // Stable sorting retains Rust's exported tie order.
 matches.sort((a:any,b:any)=>a.values[time]<b.values[time]?-1:a.values[time]>b.values[time]?1:0);
 if(trace){trace.times['json.filterSort']=performance.now()-at;Object.assign(trace.counts,{scanned,matched:matches.length,jsonBytes:new TextEncoder().encode(text).length});}
 view.objects=matches.slice(0,query.size);
 view.result.page=view.objects.map((o:any,i:number)=>({stream:o.stream,row:i,key:o.key,cached:false}));
 view.result.next=matches.length>query.size?'benchmark-next':undefined;
 view.batch={bytes:new ArrayBuffer(0),rows:new Uint32Array()};
 if(trace){trace.counts.materialized=view.objects.length;trace.counts.pageHash=digest(view.objects);}
 return view;
}
