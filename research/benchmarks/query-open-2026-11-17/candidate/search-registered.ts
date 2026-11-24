import type {ViewResponse,ViewQuery,ViewSummary} from '../pil/view.ts';
import type {TrunkBootstrap} from '../pil/trunk.ts';
import type {Archive} from '../store/protocol.ts';
import type {SearchPlan} from './planner.ts';
import type {Predicate,QueryPlan,Filter} from '../query.ts';
import type {SeedPredicate,SeedQuery,SeedPack,SeedReader} from '@trie-trunk/data-seeds';
import type {SeedRead} from '../store/registered.ts';
import {collectNative,type NativeCollectOptions} from '../pil/native-collect.ts';
import {HOUR} from '../store/time-trie.ts';
import {applySummaryPeriods} from './summary-selection.ts';
import {retainLastSeed,type WorkerBoundary} from './boundaries.ts';

function comparison(f:Filter):SeedPredicate{
 if(f.value.kind==='null'){
  if(f.op==='==')return {field:f.field,op:'is-null',value:null};
  if(f.op==='!=')return {and:[{field:f.field,op:'is-present',value:null},{not:{field:f.field,op:'is-null',value:null}}]};
 }
 return {field:f.field,op:f.op,value:{$literal:f.value}};
}
function predicate(p:Predicate):SeedPredicate{
 if(p.kind==='filter')return comparison(p.filter);
 if(p.kind==='not')return {not:predicate(p.value)};
 const a=predicate(p.left),b=predicate(p.right);
 if(p.kind==='and')return {and:[a,b]};if(p.kind==='or')return {or:[a,b]};
 return {or:[{and:[a,{not:b}]},{and:[{not:a},b]}]};
}
export function seedPredicate(query:QueryPlan|undefined,fields:string[]):SeedPredicate|undefined{
 const parts:SeedPredicate[]=(query?.filters??[]).map(comparison);
 if(query?.predicate)parts.push(predicate(query.predicate));
 if(query?.search){
  if(query.search.mode!=='contains')throw Error(`Search mode ${query.search.mode} is not supported by the Rust Seed query yet`);
  parts.push({or:fields.map(field=>({field,op:'contains',value:query.search!.text}))});
 }
 return parts.length?{and:parts}:undefined;
}

/** Existing calendar/summary planning and transport requests; Rust owns the
 * selected storage readers, filtering, global ordering, limit and materialization. */
export async function query(trunk:TrunkBootstrap,query:ViewQuery,token:string,signal:AbortSignal,
 options:Pick<NativeCollectOptions,'codec'|'definitions'|'archive'|'fetcher'>,calendar:SearchPlan,
 onPlan?:((plan:SearchPlan)=>void|Promise<void>),boundary?:WorkerBoundary):Promise<ViewResponse>{
 const trace=boundary?.trace;
 const now=()=>trace?performance.now():0,add=(name:string,start:number)=>{if(trace)trace.times[name]=(trace.times[name]??0)+now()-start;};
 const count=(name:string,n=1)=>{if(trace)trace.counts[name]=(trace.counts[name]??0)+n;};
 const yieldTask=async()=>{const start=now();await new Promise<void>(resolve=>setTimeout(resolve,0));add('yield',start);count('yields');};
 if(trace&&boundary)trace.times.beforeRegistered=now()-boundary.started;
 let at=now();const plan:SearchPlan=structuredClone(calendar);add('clone',at);at=now();
 await onPlan?.(plan);signal.throwIfAborted();
 add('summary',at);at=now();applySummaryPeriods(plan);
 const from=Date.parse(query.from),to=Date.parse(query.to),archive:Archive=options.archive;
 const branches=[...new Map(plan.branches.map(b=>[b.tap,b])).values()].filter(b=>b.eligiblePeriods===undefined||b.eligiblePeriods.length>0);
 add('selection',at);at=now();if(trace){count('branches',branches.length);count('calendarHours',branches.reduce((n,b)=>n+b.hours.length,0));count('selectedEvents',branches.reduce((n,b)=>n+b.hours.filter(h=>!(b.excludedHours??[]).includes(h.time)).reduce((n,h)=>n+h.count,0),0));}
 // Collection changes local readiness flags, not the origin query snapshot.
 let snapshot=Array.from(new Uint8Array(await crypto.subtle.digest('SHA-256',new TextEncoder().encode(JSON.stringify([trunk.version,query.tap,from,to,query.query,branches.map(b=>[b.tap,b.definition,b.receipt.generation,b.receipt.sequence,b.hours.map(h=>[h.time,h.count]),b.excludedHours??[],b.eligiblePeriods])])))),b=>b.toString(16).padStart(2,'0')).join('');
 add('snapshot',at);let offset=0;
 if(boundary)boundary.replicationStarted=performance.now();
 const live=new Map<string,SeedPack[]>();let liveBytes=0;
 const releaseLive=()=>{for(const packs of live.values())for(const pack of packs)pack.dispose();live.clear();};
 const jobs=branches.map(b=>{
  const common={...options,trace,onSeedBoundary:boundary?(seed:import('./boundaries.ts').SeedBoundary)=>retainLastSeed(boundary,seed):undefined,metadata:b.metadata,tap:b.tap,definitionVersion:b.definition,token,signal,from:query.from,to:query.to,timeline:b.receipt,indexHours:b.indexHours,
   onAttempt:()=>{for(const pack of live.get(b.tap)??[]){liveBytes-=pack.info().byteLength;pack.dispose();}live.delete(b.tap);},
   registeredLive:async(pack:SeedPack)=>{
    signal.throwIfAborted();const bytes=pack.info().byteLength;
    if(liveBytes+bytes>64*1024*1024)throw Error('Live query memory budget exhausted');
    liveBytes+=bytes;const packs=live.get(b.tap)??[];packs.push(pack.retain());live.set(b.tap,packs);
   }};
  if(trace){count('indexHours',b.indexHours.length);count('emptyIndexHours',b.indexHours.filter(h=>!h.count).length);}
  const indexReady=b.indexHours.length?collectNative({...common,data:'none'}):Promise.resolve([]);
  const ranges=plan.packRequests.filter(r=>r.tap===b.tap).map(r=>({from:r.from,to:r.to}));
  if(trace){count('collectionRanges',ranges.length);count('collectionHours',ranges.reduce((n,r)=>n+(r.to-r.from)/HOUR,0));}return ranges.length?collectNative({...common,ranges,indexReady}):indexReady;
 });
 const completions=await Promise.allSettled(jobs);
 if(boundary)boundary.replicationEnded=performance.now();
 const failed=completions.find((r):r is PromiseRejectedResult=>r.status==='rejected');if(failed){releaseLive();throw failed.reason;}
 if(signal.aborted){releaseLive();signal.throwIfAborted();}
 const reads:SeedRead[]=[],bindings:ViewResponse['bindings']=[],ends:number[]=[];
 let cursor:SeedQuery|undefined;
 const sources:(SeedReader|SeedPack)[]=[],sourceBindings:number[]=[];
 try{
  // Open-hour Packs only append. Pin pagination to the observed member counts,
  // so a later page cannot silently skip/repeat rows after new ingress.
  if(live.size){
   const revision=[...live].sort(([a],[b])=>a.localeCompare(b)).map(([tap,packs])=>[tap,packs.map(pack=>{const i=pack.info();return [String(i.hour),i.length,i.byteLength]})]);
   snapshot=Array.from(new Uint8Array(await crypto.subtle.digest('SHA-256',new TextEncoder().encode(JSON.stringify([snapshot,revision])))),b=>b.toString(16).padStart(2,'0')).join('');
  }
  if(query.after){const previous=JSON.parse(atob(query.after));if(previous.snapshot!==snapshot||!Number.isSafeInteger(previous.offset)||previous.offset<0)throw Error('Search snapshot changed; restart search');offset=previous.offset;}
  for(const b of branches){
   const hours=b.hours.filter(h=>h.count&&h.time<to&&h.time+HOUR>from&&!(b.excludedHours??[]).includes(h.time)).map(h=>h.time);
   const resident=live.get(b.tap)??[];
   if(!hours.length&&!resident.length)continue;
   bindings.push({stream:bindings.length+1,tap:b.tap,digest:b.definition});
   if(hours.length){
   const metrics:Record<string,number>|undefined=trace?{}:undefined;
   at=now();const read=(await archive(token,{kind:'seed-read',...(metrics?{metrics}:{}),query:{tap:b.tap,digest:b.definition,generation:b.receipt.generation,from,to,hours,periods:b.eligiblePeriods,...(trace?{trace:true}:{})}})).seedRead;
   if(trace)for(const [key,value] of Object.entries(metrics!))trace.times['open.'+key]=(trace.times['open.'+key]??0)+value;
   add('readerOpen',at);count('packs',hours.length);if(!read)throw Error('Missing Rust Seed reader');reads.push(read);if(trace)for(const [key,value] of Object.entries(read.timings??{}))trace.times['open.'+key]=(trace.times['open.'+key]??0)+value;
   for(let first=0,retained=0;;){signal.throwIfAborted();const start=now(),prepared=read.reader.prepare(first,64);add('prepare',start);count('prepareCalls');count('metadataBytes',prepared.bytes-retained);retained=prepared.bytes;if(prepared.done)break;first=prepared.next;await yieldTask();}
   sources.push(read.reader);ends.push((ends.at(-1)??0)+hours.length);sourceBindings.push(bindings.length);
   }
   for(const pack of resident){sources.push(pack);ends.push((ends.at(-1)??0)+1);sourceBindings.push(bindings.length);}
  }
  at=now();const metadata=[...new Map([...reads.flatMap(r=>r.definitions),...branches.filter(b=>live.has(b.tap)).map(b=>b.metadata)].map(m=>[m.digest,m])).values()];
  const fields=metadata.flatMap(m=>m.registered!.fields),time=fields.find(f=>f.annotations.some(a=>/^@time(?:\(|$)/.test(a)))?.name??fields.find(f=>['@time','timestamp','time'].includes(f.name))?.name;
  const objects:NonNullable<ViewResponse['objects']>=[];
  const registeredBranches=new Map(trunk.bindings.map(b=>[JSON.stringify([b.tap,b.digest]),b.branchId]));
  add('metadata',at);at=now();if(sources.length){
   cursor=sources[0].query(sources.slice(1));
   const filter=seedPredicate(query.query,[...new Set(fields.filter(f=>/string|tstr|STRING/i.test(f.type)).map(f=>f.name))]);
   const predicates:SeedPredicate[]=[];if(filter)predicates.push(filter);
   if(time)predicates.push({field:time,op:'>=',value:{$integer:String(from)}},{field:time,op:'<',value:{$integer:String(to)}});
   if(predicates.length)cursor.filter({and:predicates});
   if(time)cursor.orderBy(time,query.query?.order??'asc');
   cursor.offset(offset).limit(query.size+1);
   add('cursorSetup',at);while(objects.length<=query.size){
    signal.throwIfAborted();at=now();const slice=cursor.poll(Math.min(256,query.size+1-objects.length),4096);add('poll',at);count('pollCalls');count('work',slice.work);at=now();
    try{if(slice.batch){const values=slice.batch.objects(),origins=slice.batch.origins();
     for(let i=0;i<values.length;i++){
      const origin=origins[i],source=ends.findIndex(end=>origin.pack<end);
      if(source<0)throw Error('Unknown result source');
      const stream=sourceBindings[source],tap=bindings[stream-1].tap;
      objects.push({values:values[i],definition:origin.definition,stream,branchId:registeredBranches.get(JSON.stringify([tap,origin.definition])),key:`${snapshot}:${origin.pack}:${origin.row}`});
     }
    }}finally{slice.batch?.dispose();}
    add('materialize',at);if(slice.done)break;await yieldTask();
   }
  }
  if(trace&&cursor){const stats=cursor.stats();trace.counts.scanned=stats.scanned;trace.counts.matched=stats.matched;}
  count('materialized',objects.length);const more=objects.length>query.size;if(more)objects.pop();
  // An exhausted first page proves the whole query empty. It is not a missing
  // histogram, and must not fall back to the unfiltered timeline's counts.
  let summary:ViewSummary|undefined;
  if(!more&&offset===0&&!objects.length){
   const period=300_000,step=Math.max(period,Math.ceil((to-from)/128/period)*period);
   const counts=Array(Math.ceil((to-from)/step)).fill(0);
   summary={from,to,step,count:0,counts,errors:[...counts],status:'committed-snapshot'};
  }
  return {objects,definitions:metadata,bindings,batch:{bytes:new ArrayBuffer(0),rows:new Uint32Array()},result:{cursor:query.after??'',snapshot,summaryVersion:snapshot,
   ...(summary?{summary}:{}),
   page:objects.map((o,i)=>({stream:o.stream,row:offset+i,key:o.key,cached:false})),next:more?btoa(JSON.stringify({snapshot,offset:offset+objects.length})):undefined,
   committedThrough:Math.max(from,...branches.flatMap(b=>b.receipt.rows.map(h=>h.time+HOUR))),provisional:branches.some(b=>b.receipt.missing.length>0)}};
 }finally{cursor?.dispose();for(const read of reads)read.dispose();releaseLive();}
}
