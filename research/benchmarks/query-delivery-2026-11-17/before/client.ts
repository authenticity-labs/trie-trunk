import {nodeConnection} from './node-connection.ts';
import {nodeBroker} from './node-broker.ts';
import {searchDebugEnabled,receiveSearchBoundary,type SearchBoundary} from '../search/boundaries.ts';
import {INDEX_ROW_BYTES} from './pac-native.ts';
import { restoreCacheError } from "../store/errors.ts";
import {activeConnection,setActiveConnection,type ServerConnection} from '../connections.ts';
import { viewEvents } from "./view-events.ts";
import type { ViewResponse, ViewQuery, ViewResult } from "./view.ts";
import type { Collection } from "../store/protocol.ts";
import { PILEvent } from "./event.ts";
import { NativeEvent, NativeTrie } from "../seeds/index.ts";
import { valueAt } from "../dictionary.ts";
import { compile, Choices, type Metadata, type Mapping, type Binding } from "./metadata.ts";
import type { Request, Reply } from "./protocol.ts";
import type { Selection, LogEvent } from "../events.ts";
import type { HistoryQuery, ObservedState } from "../replay.ts";
let storageMessage='';
const storageListeners=new Set<(message:string)=>void>();
export function watchStoragePublication(listener:(message:string)=>void){storageListeners.add(listener);listener(storageMessage);return ()=>{storageListeners.delete(listener)}}
function storageStatus(message:string){storageMessage=message;for(const listener of storageListeners)listener(message)}
export function retryStoragePublication(token:string,signal:AbortSignal){return request({kind:'retry-storage',token},signal,()=>{})}
let transport: Pick<MessagePort,'postMessage'> | undefined;
let owner: SharedWorker | undefined;
let node: ReturnType<typeof nodeConnection> | undefined;
let next = 1;
const requests = new Map<
  number,
  { receive: (reply: Reply) => void; fail: (error: Error) => void }
>();
// Close this tab's transport, not other subscribers on the shared worker.
export function disconnectClient() {
  storageStatus('');
  node?.close();node=undefined;transport=undefined;owner=undefined;
  for (const job of [...requests.values()]) job.fail(new DOMException('Workspace closed', 'AbortError'));
  requests.clear();
}
export function selectConnection(value:ServerConnection){
 const previous=activeConnection();
 if(previous.id===value.id&&previous.url===value.url)return;
 setActiveConnection(value);
}
export function startDesktopResources(){if(document.documentElement.hasAttribute("data-snoot-desktop"))port()}
function port(): Pick<MessagePort,'postMessage'> {
  if (node) return node.port();
  const failure=(error:Error)=>{
    transport=node?.port();
    for(const job of [...requests.values()])job.fail(error);
    requests.clear();
  };
  let control:MessagePort;
  if(typeof SharedWorker!=='undefined'){
    owner=new SharedWorker(new URL('./worker.ts',import.meta.url),{type:'module'});
    control=owner.port;
    owner.onerror=()=>failure(Error('Data node broker stopped. Reload the page to reconnect.'));
  }else{
    // Platforms without SharedWorker still use the same exclusive node fence.
    const channel=new MessageChannel();nodeBroker()(channel.port2);control=channel.port1;
  }
  let dispatchMode='port';try{dispatchMode=sessionStorage.getItem('snoot.dispatch')??'port'}catch{}
  node=nodeConnection(control,()=>new Worker(new URL('./node-worker.ts',import.meta.url),{type:'module'}),{
    workerDispatch:dispatchMode==='worker'||dispatchMode==='worker-duplex',workerReplies:dispatchMode==='worker-duplex',
    desktop:document.documentElement.hasAttribute('data-snoot-desktop'),reset:failure,
    receive:(event:MessageEvent<Reply>)=>{
      if(event.data.kind==='storage-status'){storageStatus(event.data.message);return;}
      requests.get(event.data.id)?.receive(event.data);
    },
  });
  transport=node.port();
  addEventListener('pagehide',disconnectClient,{once:true});
  return transport;
}

function request(
  command:
    | {kind:"query-event";event:import("@trie-trunk/data-seeds").QueryEvent;token:string}
    | {kind:'time-window';query:import('./time-window.ts').TimeWindowRequest;token:string}
    | {kind:'retry-storage';token:string}
    | {kind:'trunk';token:string;folder?:string}
    | { kind:"fast-dashboard"; request:import('../fast-dashboard.ts').FastRequest; token:string }
    | { kind:"prime-summaries"; service:string; recent:boolean; token:string }
    | { kind:"persist-view"; definitions:Metadata[]; bindings:Binding[]; token:string }
    | { kind:"remember-view"; view:ViewResponse; token:string }
    | { kind:"view"; query:ViewQuery; token:string; progressive?:boolean;measureBoundary?:boolean;traceBoundary?:boolean }
    | { kind:"rebase"; sources:string[]; token:string }
    | { kind: "schemas"; tap: string; token: string }
    | { kind: "history"; query: HistoryQuery; token: string }
    | {
        kind: "events";
        selection: Selection;
        token: string;
        collection?: string;
      }
    | { kind: "begin"; collection: Collection; token: string }
    | { kind: "restore"; collection?: string; token: string },
  signal: AbortSignal,
  receive: (reply: Reply) => void,
  targetConnection?:ServerConnection,
  onDispatch?:()=>void,
  trace?:Record<string,number>,
): Promise<void> {
  signal.throwIfAborted();
  if(trace)trace.portStartAt=performance.timeOrigin+performance.now();
  const connection = port(),
    id = next++;
  if(trace)trace.portReadyAt=performance.timeOrigin+performance.now();
  return new Promise((resolve, reject) => {
    const finish = (error?: unknown) => {
      requests.delete(id);
      signal.removeEventListener("abort", cancel);
      error ? reject(error) : resolve();
    };
    const cancel = () => {
      connection.postMessage({ kind: "cancel", id });
      finish(signal.reason ?? new DOMException("Canceled", "AbortError"));
    };
    signal.addEventListener("abort", cancel, { once: true });
    requests.set(id, {
      fail: finish,
      receive(reply) {
        try {
          if (reply.kind === "error") {
            finish(restoreCacheError(reply.message,reply.cacheIssue));
            return;
          }
          if (reply.kind === "done") {
            finish();
            return;
          }
          receive(reply);
          if (reply.kind === "batch" || reply.kind === "native-batch") {
            // Release the next batch on a new task, without capping throughput
            // to one network chunk per animation frame. Only one port batch
            // is in flight; the tab already owns these PIL buffers.
            setTimeout(() => {
              if (requests.has(id)) connection.postMessage({ kind: "ack", id });
            });
          }
        } catch (error) {
          connection.postMessage({ kind: "cancel", id });
          finish(error);
        }
      },
    });
    onDispatch?.();
    const message:Request={...command,id,connection:targetConnection??activeConnection()};
    if(trace&&message.kind==='view'){
      trace.postStartAt=performance.timeOrigin+performance.now();trace.postWallStartAt=Date.now();
      message.dispatch={clientSentAt:trace.postStartAt,clientWallSentAt:trace.postWallStartAt};
    }
    connection.postMessage(message);
    if(trace){trace.postEndAt=performance.timeOrigin+performance.now();queueMicrotask(()=>{trace.submitMicrotaskAt=performance.timeOrigin+performance.now()});setTimeout(()=>{trace.submitTaskAt=performance.timeOrigin+performance.now()},0);}
  });
}
export async function loadDictionaries(
  tap: string,
  token: string,
  signal: AbortSignal,
): Promise<Choices> {
  let result: Choices | undefined;
  await request({ kind: "schemas", tap, token }, signal, (reply) => {
    if (reply.kind === "schemas")
      result = new Choices(reply.definitions, reply.bindings);
  });
  if (!result) throw Error("Missing PIL schema negotiation");
  return result;
}
export async function readTimeWindow(query:import('./time-window.ts').TimeWindowRequest,token:string,signal:AbortSignal,onProgress?:(result:import('./time-window.ts').TimeWindowResult)=>void){
 let result:import('./time-window.ts').TimeWindowResult|undefined;
 await request({kind:'time-window',query,token},signal,reply=>{if(reply.kind==='time-window'){result=reply.result;onProgress?.(result);}});
 if(!result)throw Error('Missing time window result');return result;
}
export async function loadTrunk(connection:ServerConnection,token:string,signal:AbortSignal,folder?:string,onSnapshot?:(trunk:import('./trunk.ts').TrunkBootstrap)=>void):Promise<import('./trunk.ts').TrunkBootstrap>{
 let result:import('./trunk.ts').TrunkBootstrap|undefined;
 await request({kind:'trunk',token,folder},signal,reply=>{if(reply.kind==='trunk'){result=reply.trunk;onSnapshot?.(result);}},connection);
 if(!result)throw Error('Missing trunk bootstrap');return result;
}
export async function streamEvents(
  selection: Selection,
  token: string,
  signal: AbortSignal,
  receive: (events: LogEvent[]) => void,
  connected: () => void,
  onDictionaries?: (dictionaries: Choices) => void,
  collection?: string,
) {
  return receiveEvents(
    { kind: "events", selection, token, collection },
    signal,
    receive,
    connected,
    onDictionaries,
  );
}
async function receiveEvents(
  command:
    | {kind:"query-event";event:import("@trie-trunk/data-seeds").QueryEvent;token:string}
    | { kind:"prime-summaries"; service:string; recent:boolean; token:string }
    | { kind: "history"; query: HistoryQuery; token: string }
    | {
        kind: "events";
        selection: Selection;
        token: string;
        collection?: string;
      }
    | { kind: "restore"; token: string; collection?: string },
  signal: AbortSignal,
  receive: (events: LogEvent[]) => void,
  connected: () => void,
  onDictionaries?: (dictionaries: Choices) => void,
  onCollection?: (
    collection: Collection,
    coverage: { from: number; to: number }[],
  ) => void,
  onHistory?: (state: ObservedState) => void,
) {
  const definitions = new Map<string, Metadata>();
  const streams = new Map<number, Mapping>();
  const native = new Map<string, NativeTrie>();
  let headers: string[] = [];
  await request(command, signal, (reply) => {
    if (reply.kind === "history-state") onHistory?.(reply.state);
    if (reply.kind === "collection")
      onCollection?.(reply.collection, reply.coverage);
    if (reply.kind === "schemas") {
      for (const d of reply.definitions) definitions.set(d.digest, d);
      onDictionaries?.(new Choices(reply.definitions, reply.bindings));
    } else if (reply.kind === "native-source") {
      const trie = new NativeTrie(compile(reply.definition,reply.binding.tap,headers));
      for(const chunk of reply.trie)trie.append(chunk.start,chunk.bytes);
      native.set(reply.source,trie);
    } else if(reply.kind === "native-batch") {
      const trie=native.get(reply.source);
      if(!trie)throw Error("Missing native source dependencies");
      const {rows,payload,offsets}=reply.page;
      const events:LogEvent[]=[];
      for(let i=0;i<offsets.length;i+=2){
        const event=new NativeEvent(rows.subarray(i/2*INDEX_ROW_BYTES,(i/2+1)*INDEX_ROW_BYTES),payload.subarray(offsets[i],offsets[i+1]),trie,reply.page.common?.[i/2],reply.page.bases[i/2]);
        if(reply.where.every(([name,value])=>valueAt(event,name)===value))events.push(event);
      }
      receive(events);
    } else if (reply.kind === "definition")
      definitions.set(reply.definition.digest, reply.definition);
    else if (reply.kind === "binding") {
      const b = reply.binding;
      const d = definitions.get(b.digest);
      if (!d) throw Error("Missing PIL dictionary");
      streams.set(b.stream, compile(d, b.tap, headers));
    } else if (reply.kind === "connected") {
      headers = reply.headers;
      connected();
    } else if (reply.kind === "batch") {
      const { bytes, rows } = reply.batch;
      const events: LogEvent[] = [];
      for (let i = 0; i < rows.length; i += 3) {
        const mapping = streams.get(rows[i]);
        if (!mapping) throw Error("Missing PIL stream binding");
        events.push(
          new PILEvent(
            new Uint8Array(bytes, rows[i + 1], rows[i + 2] - rows[i + 1]),
            mapping,
            reply.batch.common?.[i/3],
          ),
        );
      }
      receive(events);
    }
  });
  return true;
}

// Retained write history is not a full object snapshot. The returned token is
// evidence of the observed invalidation state, not proof that a replay base
// exists or that the object is still current after this request completes.
export async function streamObjectHistory(
  query: HistoryQuery, token: string, signal: AbortSignal,
  receive: (events: LogEvent[]) => void,
): Promise<ObservedState> {
  let state: ObservedState | undefined;
  await receiveEvents({ kind: "history", query, token }, signal, receive, () => {},
    undefined, undefined, observed => state = observed);
  if (!state) throw Error("Missing completed history state");
  return state;
}

export function beginCollection(
  collection: Collection,
  token: string,
  signal: AbortSignal,
) {
  return request({ kind: "begin", collection, token }, signal, () => {});
}
export function rebaseCollection(source:string,token:string,signal:AbortSignal){
  return rebaseCollections([source],token,signal);
}
export function rebaseCollections(sources:string[],token:string,signal:AbortSignal){
  return request({kind:"rebase",sources,token},signal,()=>{});
}
export function restoreCollection(
  token: string,
  collection: string | undefined,
  signal: AbortSignal,
  receive: (events: LogEvent[]) => void,
  onCollection: (
    collection: Collection,
    coverage: { from: number; to: number }[],
  ) => void,
) {
  return receiveEvents(
    { kind: "restore", token, collection },
    signal,
    receive,
    () => {},
    undefined,
    onCollection,
  );
}

/** A bounded PIL page is not evidence of complete native replication coverage. */
export type SearchExecutionResult = {result:ViewResult;events:LogEvent[];view:ViewResponse};

export function executeSearch(query:ViewQuery,token:string,signal:AbortSignal,onPage?:(page:SearchExecutionResult)=>void,onDataset?:(summary:import("./view.ts").ViewSummary|undefined,error?:string)=>void,onDone?:()=>void,onDispatch?:()=>void,boundary?:SearchBoundary):Promise<SearchExecutionResult> {
  boundary??={submitted:performance.now()};
  const measurement=boundary,client=searchDebugEnabled()?(boundary.client??={}):undefined,connection=activeConnection();let reported=false;
  const report=(status:'complete'|'error'|'cancelled',rows:number,error?:string)=>{
   if(reported||query.preview)return;reported=true;
   const finish=()=>{
    const steps=measurement.steps,received=measurement.received,finished=performance.now();
    submitQueryEvent({time:Date.now(),id:Array.from(crypto.getRandomValues(new Uint8Array(10))),source:query.tap,
     query:query.text??JSON.stringify(query.query??{}),status,...(error?{error:error.slice(0,16384)}:{}),user_agent:navigator.userAgent,
     from:Date.parse(query.from),to:Date.parse(query.to),rows,duration:finished-measurement.submitted,
     worker:measurement.workerMs,worker_start:steps?.startedAt===undefined?undefined:Math.max(0,steps.startedAt-(performance.timeOrigin+measurement.submitted)),before_collection:steps?.beforeReplicationMs,collection:steps?.replicationMs,
     after_collection:steps?.afterReplicationMs,presentation:received===undefined?undefined:finished-received},token,connection);
   };
   // Observe the first presentation opportunity after the result reaches the UI.
   // A hidden page has no paint opportunity and reports delivery instead.
   if(document.visibilityState==='visible')requestAnimationFrame(()=>requestAnimationFrame(finish));else finish();
  };
  return new Promise((resolve,reject)=>{
    let output:SearchExecutionResult|undefined,ready=false;
    const complete=()=>{if(!output)throw Error("Missing event page");ready=true;resolve(output);report('complete',output.events.length)};
    void request({kind:"view",query,token,progressive:!!onPage,...(client?{traceBoundary:true}:{}),...(boundary?{measureBoundary:true}:{})},signal,reply=>{
      if(reply.kind==='dispatch-received'){if(client){client.ackReceivedAt=performance.timeOrigin+performance.now();client.ackWallReceivedAt=Date.now();Object.assign(client,reply.dispatch);}return;}
      if(client&&(reply.kind==='view-dataset'||reply.kind==='view-page'||reply.kind==='view'||reply.kind==='view-summary'))client[reply.kind+'At']=performance.timeOrigin+performance.now();
      if(boundary&&(reply.kind==="view"||reply.kind==="view-summary")&&reply.boundaryWorkerMs!==undefined)receiveSearchBoundary(boundary,reply.boundaryWorkerMs,reply.boundarySteps);
      if(reply.kind==="view-dataset"){const start=client?performance.now():0;onDataset?.(reply.summary,reply.error);if(client)client.datasetMs=(client.datasetMs??0)+performance.now()-start;}
      else if(reply.kind==="view"||reply.kind==="view-page") {
        let start=client?performance.now():0;const events=viewEvents(reply.view);if(client)client.convertMs=(client.convertMs??0)+performance.now()-start;start=client?performance.now():0;
        output={result:reply.view.result,events,view:reply.view};
        if(reply.kind==="view-page")onPage?.(output);else complete();if(client)client.pageMs=(client.pageMs??0)+performance.now()-start;
      } else if(reply.kind==="view-summary"&&output) {
        output={...output,result:reply.result,view:{...output.view,result:reply.result}};
        complete();
      }
    },undefined,boundary?()=>{onDispatch?.();boundary.sent=true;if(client)client.dispatchAt=performance.timeOrigin+performance.now()}:onDispatch,client).then(()=>{if(!ready)complete()},error=>{report(signal.aborted?'cancelled':'error',output?.events.length??0,String(error));if(!ready)reject(error);else if(!signal.aborted)onDataset?.(undefined,String(error))}).finally(onDone).catch(reject);
  });
}

export function rememberView(view:ViewResponse,token:string,signal:AbortSignal) {
  return request({kind:"remember-view",view,token},signal,()=>{});
}

/** Durable hour promotion is separate from delivering a visible search page. */
export function persistView(view:ViewResponse,token:string,signal:AbortSignal) {
  return request({kind:"persist-view",definitions:view.definitions,bindings:view.bindings,token},signal,()=>{});
}

export function primeSummaries(service:string, recent:boolean, token:string, signal:AbortSignal, onProgress:(progress:import('../summary-replication.ts').SummaryProgress)=>void) {
  return request({kind:'prime-summaries',service,recent,token},signal,reply=>{
    if(reply.kind==='summary-progress')onProgress(reply.progress);
  });
}

export async function loadFastDashboard(query:import('../fast-dashboard.ts').FastRequest,token:string,signal:AbortSignal) {
  let result:import('../fast-dashboard.ts').FastResult|undefined;
  await request({kind:'fast-dashboard',request:query,token},signal,reply=>{if(reply.kind==='fast-dashboard')result=reply.result});
  if(!result)throw Error('Missing dashboard summaries.');
  return result;
}

let pendingQueryEvents=0;
function submitQueryEvent(event:import('@trie-trunk/data-seeds').QueryEvent,token:string,connection:ServerConnection){
 if(pendingQueryEvents>=8){console.warn('Query telemetry queue is full');return;}
 pendingQueryEvents++;
 void request({kind:'query-event',event,token},AbortSignal.timeout(15000),()=>{},connection)
  .catch(error=>console.warn('Query telemetry was not acknowledged',error))
  .finally(()=>{pendingQueryEvents--;});
}
