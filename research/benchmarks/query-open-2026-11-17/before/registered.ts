import type {Packs,SeedTimeline,SeedReader,SeedLeaves,SeedBranches} from '@trie-trunk/data-seeds';
import type {CollectedBatch} from '../seeds/collection.ts';
import type {CodecTrunkAPI} from '../trunk-api.ts';
import {seedFiles} from '../trunk-api.ts';
import {NativeStore,type NativeBackend,type NativeSource} from './native.ts';
import {seedTransaction} from './seed-writer.ts';
import {TIME_BEDS,TIME_INDEX,TIME_STATE,timeLeaf} from './time-trie.ts';
import type {registeredDefinitions} from './trunk.ts';

export type SeedSourceQuery={tap:string;digest:string;generation?:string};
export type SeedPublication=SeedSourceQuery&{serial:number;batch:CollectedBatch};
export type SeedAccess={trie:CodecTrunkAPI;branches:SeedBranches;source:NativeSource;packs:Packs;time:SeedTimeline;dispose():void};
export type SeedRead={timings?:Record<string,number>;reader:SeedReader;hours:number[];definitions:import('../pil/metadata.ts').Metadata[];dispose():void};

/** File ownership adapter. Registry selection, Pack layout, index restoration,
 * query evaluation, and positional writes execute in the shared Rust runtime. */
export class RegisteredStorage {
 constructor(private backend:NativeBackend,private registry:ReturnType<typeof registeredDefinitions>,private namespace:string){}
 async source(query:SeedSourceQuery,timings?:Record<string,number>):Promise<SeedAccess>{
  const now=()=>timings?performance.now():0;let at=now();
  const source=await new NativeStore(this.backend).seedSource(query);
  if(timings)timings['source.snapshot']=now()-at;at=now();
  const lease=await this.registry.acquire(source.registeredDefinition!,source.tap);
  if(timings)timings['source.registry']=now()-at;at=now();
  let packs:Packs|undefined,time:SeedTimeline|undefined;
  try{
   packs=lease.branches.packs();
   if(timings)timings['source.packs']=now()-at;at=now();
   time=lease.branches.time(source.timeTrie!.bytes as Uint8Array<ArrayBuffer>,source.timeTrie!.ranges.map(r=>[r.from,r.to] as const));
   if(timings)timings['source.time']=now()-at;
   let closed=false;
   return {trie:lease.trie,branches:lease.branches,source,packs,time,dispose(){if(closed)return;closed=true;time!.dispose();packs!.dispose();lease.release();}};
  }catch(error){time?.dispose();packs?.dispose();lease.release();throw error;}
 }
 async publish(input:SeedPublication){
  const source=await new NativeStore(this.backend).seedSource(input);
  const lease=await this.registry.acquire(source.registeredDefinition!,source.tap);
  try{
   return await seedTransaction(this.backend,seedFiles,lease.trie.commits(),async write=>{
    const source=await new NativeStore(write.backend).publishSeedPacks(input);
    const file=await write.file(source.key,'beds');
    const plan=input.batch.replica.batch(lease.trie,file,input.batch.members.length+8);
    try{
     for(const {pack,placement} of input.batch.members)plan.load(placement.beds,pack);
     plan.footer();return {plan,result:[source.key]};
    }catch(error){plan.dispose();throw error;}
   },{fileScope:this.namespace});
  }finally{lease.release();}
 }
 async read(query:SeedSourceQuery&{trace?:boolean;from:number;to:number;hours?:number[];periods?:[number,number][]}):Promise<SeedRead>{
  const timings:Record<string,number>|undefined=query.trace?{}:undefined;const now=()=>timings?performance.now():0;let at=now();
  const access=await this.source(query,timings),handles:{id:number;close():void}[]=[],leaves:SeedLeaves[]=[];
  if(timings)timings.source=now()-at;let reader:SeedReader|undefined;
  const close=()=>{reader?.dispose();for(const leaf of leaves)leaf.dispose();for(const h of handles)h.close();access.dispose();};
  try{
   const hours=query.hours??access.time.range(query.from,query.to).filter(h=>h.count!==0&&h.count!==0n).map(h=>h.hour);
   const addresses=hours.map(hour=>{
    const leaf=timeLeaf(access.source.timeTrie,hour),required=TIME_BEDS|TIME_INDEX|TIME_STATE;
    if(!leaf||(leaf.flags&required)!==required)throw Error('Selected Seed hour is not collected');
    return leaf.beds;
   });
   const bind=async(kind:'beds'|'index')=>{
    const file=await this.backend.file(access.source.key,kind);
    const routed=kind==='index'?{...file,read:(out:Uint8Array,{at}:{at:number})=>{
     let done=0;
     while(done<out.length){const offset=at+done,e=access.source.files.index.extents.find(e=>e.Start<=offset&&offset<e.End);
      if(!e)throw Error('Missing collected index extent');
      const n=file.read(out.subarray(done,done+Math.min(out.length-done,e.End-offset)),{at:e.local+offset-e.Start});
      if(!n)throw Error('Truncated collected index');done+=n;
     }return done;
    }}:file;
    const handle=seedFiles.bind(routed,kind==='beds'?JSON.stringify([this.namespace,access.source.key,kind,0]):undefined);handles.push(handle);return handle.id;
   };
   at=now();reader=access.packs.reader(await bind('beds'),access.source.files.beds.end,addresses);
   if(timings)timings.reader=now()-at;at=now();if(hours.length){
    const raw=await this.backend.file(access.source.key,'trie');
    try{
     const input={byteLength:access.source.files.trie.end,read:(out:Uint8Array)=>raw.read(out,{at:0})};
     for(const registration of access.trie.definitionGraph().resources){
      if(access.trie.route(registration.branch).source!==access.source.tap)continue;
      const scope=access.trie.trie().branches().select([registration.branch]).leaves();
      const definition=access.trie.definition(registration.definition);
      const hex=definition.digest.map(b=>b.toString(16).padStart(2,'0')).join('');
      if(scope.definitions(input).includes(hex))leaves.push(scope.read(input));
     }
    }finally{raw.close();}
    if(timings)timings.leaves=now()-at;at=now();reader.index(await bind('index'),Math.max(0,...access.source.files.index.extents.map(e=>e.End)),access.time,hours,leaves,'beds');
   }
   if(timings)timings.index=now()-at;at=now();const ranged=reader.range(query.from,query.to);reader.dispose();reader=ranged;
   if(query.periods){const selected=reader.ranges(query.periods);reader.dispose();reader=selected;}
   if(timings)timings.range=now()-at;at=now();const definitions=await Promise.all(access.trie.definitionGraph().resources.filter(r=>access.trie.route(r.branch).source===access.source.tap).map(async r=>{
    const metadata=await this.backend.definition!(r.definition);return {...metadata,registered:{...metadata.registered!,...access.trie.describe(r.definition)}};
   }));
   if(timings)timings.definitions=now()-at;let released=false;return {timings,reader,hours,definitions,dispose(){if(released)return;released=true;close();}};
  }catch(error){close();throw error;}
 }
}
