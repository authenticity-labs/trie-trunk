import {timed,timedSync,type Timings} from './timing.ts';
import type {FileIO} from './wasm.ts';
import {readPublishedFile} from './publication.ts';
import {seedRuntime} from '../trunk-api.ts';
import { NativeStore, type NativeBackend, type NativeSource, type NativeControl } from './native.ts';
import { directory, handle, entries } from './files.ts';
import {timeFileRanges,type ClientTimeTrie,type TimeRange} from './time-trie.ts';

/** Bounded decoded immutable revisions. The commit root is re-read under every
 * lease; a different directory identity or revision can never reuse a snapshot. */
/** One bounded LRU budget shared by connection-specific caches. */
export class NativeSnapshotPool {
 private entries=new Map<string,{source:NativeSource;size:number}>();private size=0;
 readonly limit:number;
 constructor(limit:number){this.limit=limit;}
 get(key:string){const entry=this.entries.get(key);if(entry){this.entries.delete(key);this.entries.set(key,entry);}return entry?.source;}
 put(key:string,source:NativeSource,size:number){
  this.remove(key);
  if(size>this.limit)return;
  this.entries.set(key,{source,size});this.size+=size;
  while(this.size>this.limit)this.remove(this.entries.keys().next().value!);
 }
 private remove(key:string){const old=this.entries.get(key);if(old){this.size-=old.size;this.entries.delete(key);}}
 retain(prefix:string,sources?:Record<string,number>){
  for(const id of this.entries.keys())if(id.startsWith(prefix)){
   const key=id.slice(prefix.length),at=key.lastIndexOf(':');
   if(!sources||sources[key.slice(0,at)]!==Number(key.slice(at+1)))this.remove(id);
  }
 }
}
let nextSnapshotScope=0;
export class NativeSnapshotCache {
 private pool:NativeSnapshotPool;private prefix=`${++nextSnapshotScope}/`;
 readonly limit:number;
 readers=0;
 private handles=new Map<string,{promise:Promise<FileIO>;users:number}>();
 async file(key:string,open:()=>Promise<FileIO>):Promise<FileIO>{
  let entry=this.handles.get(key);
  if(!entry){entry={promise:open(),users:0};this.handles.set(key,entry)}
  entry.users++;const current=entry;
  let raw:FileIO;
  try{raw=await entry.promise}catch(error){if(--entry.users===0)this.handles.delete(key);throw error}
  let closed=false;
  return {getSize:()=>raw.getSize(),read:(b,o)=>raw.read(b,o),write:(b,o)=>raw.write(b,o),truncate:n=>raw.truncate(n),flush:()=>raw.flush(),close:()=>{if(closed)return;closed=true;if(--current.users===0){this.handles.delete(key);raw.close()}}};
 }
 constructor(budget:number|NativeSnapshotPool=8*1024*1024){this.pool=typeof budget==='number'?new NativeSnapshotPool(budget):budget;this.limit=this.pool.limit;}
 get(key:string){return this.pool.get(this.prefix+key);}
 put(key:string,source:NativeSource,size:number){this.pool.put(this.prefix+key,source,size);}
 retain(sources:Record<string,number>){this.pool.retain(this.prefix,sources);}
 clear(){this.pool.retain(this.prefix);}
}
type Commit = { serial:number; sources:Record<string,number>; control:NativeControl };
const empty = ():Commit=>({serial:0,sources:{},control:{serial:0,scopes:{}}});
const optional=(dir:FileSystemDirectoryHandle,name:string)=>readPublishedFile(dir,name,true);
// Compare detached working metadata with the immutable committed snapshot.
// Avoid serializing unchanged hour directories just to discover that no new
// bytes or completion flags were published. This never compares event payloads.
function sameMetadata(a:unknown,b:unknown):boolean {
 if(a===b)return true;
 if(a===null||b===null||typeof a!=='object'||typeof b!=='object')return false;
 if(a instanceof Uint8Array||b instanceof Uint8Array){
  if(!(a instanceof Uint8Array)||!(b instanceof Uint8Array)||a.length!==b.length)return false;
  for(let i=0;i<a.length;i++)if(a[i]!==b[i])return false;
  return true;
 }
 if(Array.isArray(a)||Array.isArray(b)){
  if(!Array.isArray(a)||!Array.isArray(b)||a.length!==b.length)return false;
  for(let i=0;i<a.length;i++)if(!sameMetadata(a[i],b[i]))return false;
  return true;
 }
 const ap=Object.getPrototypeOf(a),bp=Object.getPrototypeOf(b);
 if(ap!==null&&ap!==Object.prototype||bp!==null&&bp!==Object.prototype)return false;
 const left=a as Record<string,unknown>,right=b as Record<string,unknown>;
 for(const key in left)if(Object.hasOwn(left,key)&&(!Object.hasOwn(right,key)||!sameMetadata(left[key],right[key])))return false;
 for(const key in right)if(Object.hasOwn(right,key)&&!Object.hasOwn(left,key))return false;
 return true;
}
export {atomicFile} from './atomic-file.ts';
import {atomicFile,stage} from './atomic-file.ts';
async function stageTime(dir:FileSystemDirectoryHandle,name:string,tree:ClientTimeTrie|undefined){
 return stage(dir,name,tree?.bytes.length??0,tree?timeFileRanges(tree):[]);
}
/** The surrounding worker holds the origin-wide file lease. Immutable branch
 * snapshots are published together by one atomic commit-root replacement.
 * Create a backend per leased command; commit roots never cross a lease.
 * Only immutable decoded snapshots can be reused after revision checks.
 * branch.time is the raw time trie; layout.pil stores allocation and decoding
 * context only. No package manifests or duplicate coverage ranges are retained. */
export function nativeBackend(root:FileSystemDirectoryHandle,cache?:NativeSnapshotCache,metrics?:Timings):NativeBackend {
 let committed:Promise<Commit>|undefined;
 const load=():Promise<Commit>=>committed??=(async()=>{
  const raw=await timed(metrics,'storage.commitRead',()=>optional(root,'commit.pil'));
  const runtime=raw?.length?await seedRuntime():undefined;
  const state=runtime?timedSync(metrics,'storage.commitDecode',()=>runtime.readControl(raw!) as Commit):empty();
  cache?.retain(state.sources);return state;
 })();
 const dirs=new Map<string,Promise<FileSystemDirectoryHandle>>();
 const branch=(key:string)=>{if(!/^[a-f0-9]{64}$/.test(key))throw Error('Invalid native source');let dir=dirs.get(key);if(!dir){dir=directory(root,'branches',key);dirs.set(key,dir);}return dir;};
 const snapshots=new Map<string,Promise<NativeSource>>();
 const readFileSource=async(key:string,revision:number):Promise<NativeSource>=>{
  const cached=cache?.get(`${key}:${revision}`);if(cached)return cached;
  const dir=await branch(key),raw=await optional(dir,`layout.${revision}.pil`);
  if(!raw)throw Error('Missing committed branch layout');
  const {timeRanges,...source}=(await seedRuntime()).readControl(raw) as NativeSource & {timeRanges?:TimeRange[]};
  if(source.key!==key)throw Error('Branch identity mismatch');
  const trie=await optional(dir,`branch.${revision}.time`);if(!trie)throw Error('Missing committed time trie');if(trie.length)source.timeTrie={bytes:trie,ranges:timeRanges??[]};
  for(const context of source.contexts??[]){const p=context.placement;
   for(const name of ['from','to','firstRow','rows','memberStart','memberBytes','retiredRows','retiredMembers','retiredPacks'] as const)
    if(name in p)(p as unknown as Record<string,unknown>)[name]=BigInt((p as unknown as Record<string,number|bigint>)[name]);
   for(const page of p.pages){page.row=BigInt(page.row);page.page=BigInt(page.page);}
  }
  cache?.put(`${key}:${revision}`,source,raw.byteLength*4+(trie?.byteLength??0)+1024);
  return source;
 };
 const snapshot=(key:string,revision:number)=>{const id=`${key}:${revision}`;let source=snapshots.get(id);if(!source){source=readFileSource(key,revision);snapshots.set(id,source);}return source;};
 const read=(key:string,revision:number)=>snapshot(key,revision).then(s=>structuredClone(s));
 return {
  metrics,
  async get(key){const state=await load();return state.sources[key]===undefined?undefined:read(key,state.sources[key]);},
  async list(){const state=await load();return Promise.all(Object.entries(state.sources).map(([key,n])=>read(key,n)));},
  async control(){return structuredClone((await load()).control); },
  async put(sources,control){
   const old=await load(),changed:NativeSource[]=[];
   for(const source of sources){
    const revision=old.sources[source.key];
    if(revision===undefined||!sameMetadata(source,await snapshot(source.key,revision)))changed.push(source);
   }
   if(!changed.length&&(!control||sameMetadata(control,old.control)))return;
   const next:Commit={serial:old.serial+1,sources:{...old.sources},control:control??old.control};
   if(!Number.isSafeInteger(next.serial))throw Error('Local commit sequence exhausted');
   const weights=new Map<string,number>();
   for(const source of changed){
    const dir=await branch(source.key),{timeTrie,...layout}=source;
    const bytes=(await seedRuntime()).writeControl({...layout,...(timeTrie?{timeRanges:timeTrie.ranges}:{})});
    // Independent immutable files share one publication barrier. Settle both
    // before reporting failure, so a retry cannot race an unfinished write.
    const written=await Promise.allSettled([
     timed(metrics,'storage.layoutWrite',()=>stage(dir,`layout.${next.serial}.pil`,bytes.length,[{at:0,bytes}])),
     timed(metrics,'storage.timeWrite',()=>stageTime(dir,`branch.${next.serial}.time`,timeTrie)),
    ]);
    for(const result of written)if(result.status==='rejected')throw result.reason;
    weights.set(source.key,bytes.byteLength*4+(timeTrie?.bytes.byteLength??0)+1024);
    next.sources[source.key]=next.serial;
   }
   await timed(metrics,'storage.commitWrite',async()=>atomicFile(root,'commit.pil',(await seedRuntime()).writeControl(next)));
   committed=Promise.resolve(next);
   for(const source of changed){
    const id=`${source.key}:${next.serial}`,saved=structuredClone(source);
    snapshots.set(id,Promise.resolve(saved));cache?.put(id,saved,weights.get(source.key)!);
   }
   // Only remove unreachable metadata after publication. Data-file reclamation
   // remains ordered by the selected branch state.
   for(const source of changed){
    const dir=await branch(source.key),keep=next.sources[source.key];
    if(cache?.readers)continue;
    await timed(metrics,'storage.cleanup',async()=>{for await(const entry of entries(dir))if(/^(layout\.\d+\.pil|branch\.\d+\.time)$/.test(entry.name)&&entry.name!==`layout.${keep}.pil`&&entry.name!==`branch.${keep}.time`)
     await dir.removeEntry(entry.name).catch(()=>{});});
   }
  },
  async file(key,kind,revision=0){
   const names={map:'resource.rx',trie:'leaf.trie',state:'collection.state',index:'events.indx',beds:'events.beds',sum:'branch.summ'};
   const open=async()=>handle(await branch(key),names[kind]+(kind==='state'&&revision?`.${revision}`:''));
   const file=await timed(metrics,'storage.fileOpen',()=>cache?cache.file(`${key}:${kind}:${revision}`,open):open());
   return metrics?{...file,write:(b,o)=>timedSync(metrics,'storage.dataWrite',()=>file.write(b,o)),flush:()=>timedSync(metrics,'storage.flush',()=>file.flush())}:file;
  },
  async removeFiles(key){const dir=await branch(key);for await(const entry of entries(dir))if(!/^(layout\.\d+\.pil|branch\.\d+\.time)$/.test(entry.name))await dir.removeEntry(entry.name,{recursive:true});},
  async removeOldStateFiles(key,revision){if(cache?.readers)return;const dir=await branch(key),keep=revision?`collection.state.${revision}`:'collection.state';for await(const entry of entries(dir))if(/^collection\.state(?:\.\d+)?$/.test(entry.name)&&entry.name!==keep)await dir.removeEntry(entry.name);},
 };
}
export const nativeFiles=(root:FileSystemDirectoryHandle)=>new NativeStore(nativeBackend(root));
