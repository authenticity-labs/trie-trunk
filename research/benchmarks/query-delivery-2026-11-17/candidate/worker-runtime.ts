import {workerSteps,type WorkerBoundary} from '../search/boundaries.ts';
import {searchTasks} from '../search/planner.ts';
import {matchesSource,sourceScope} from '../source.ts';
import {resourceURL} from '../resources.ts';
import { createAbortScope } from '../abort.ts';
import {INDEX_ROW_BYTES} from './pac-native.ts';
import { decodeObject } from "./control.ts";
import { nativeCacheIssue } from "../store/errors.ts";
import { SummaryReplication } from "../summary-replication.ts";
import { FastProjection, dayMs, validateFastRequest } from '../fast-dashboard.ts';
import type { Selection } from "../events.ts";
import { ViewCache } from "./view.ts";
import type { PILDefinition } from "./layout.ts";
import type { Archive, NativeSelection } from "../store/protocol.ts";
import { conditions } from "../store/conditions.ts";
import { collectNative } from "./native-collect.ts";
import { Codec } from "./wasm.ts";
import {
  PILStream,
  consume,
  checkResponse,
  pilType,
  type StoredDefinition,
} from "./stream.ts";
import type { Binding, Metadata } from "./metadata.ts";
import type { Request, Reply } from "./protocol.ts";
import { collectionURL } from "./selection.ts";
import { historyURL, observedState } from "../replay.ts";
import {prepareCalendarPlan} from '../search/calendar-plan.ts';
import * as time from './time-window.ts';
import {readTrunk,prepareTrunk,type TrunkSnapshot} from './trunk.ts';
import type {SeedDefinition,SeedRuntime} from '@trie-trunk/data-seeds';
import {openTrunk,seedRuntime} from '../trunk-api.ts';
import {registeredFetch} from '../registered-fetch.ts';
import {TrunkMetadata} from './trunk-cache.ts';

export function workerService(
  modulePromise: Promise<WebAssembly.Module>,
  transport: typeof fetch = fetch,
  archive?: Archive,
  openRegistry= openTrunk,
  controlRuntime?:SeedRuntime,
) {
  type Session = {
    views: ViewCache;
    summaries: SummaryReplication;
    codec: Codec;
    definitions: Map<string, StoredDefinition>;
    seedDefinitions:Map<string,SeedDefinition>;
    seedMetadata:Map<string,Metadata>;
    bindings: Binding[];
    negotiation: string;
    native: boolean;
    loading?: Promise<void>;
    trunks:Map<string,TrunkMetadata>;
  };
  const sessions = new Map<string, Promise<Session>>();
  const credentials = new Map<
    string,
    Promise<Pick<Session, "codec" | "definitions" | "views" | "summaries" | "seedDefinitions" | "seedMetadata">>
  >();
  const fetcher=registeredFetch(transport,async token=>(await credentials.get(token))?.views.catalog);
  const headers = (token: string) => ({
    Accept: pilType,
    "PIL-Version": "2",
    "PIL-Features": "adaptive-integers",
    "Trunk-Completion": "body",
    ...(token ? { Authorization: `Bearer ${token}` } : {}),
  });
  async function session(tap: string, token: string, storage = archive) {
    const key = JSON.stringify([tap, token]);
    let value = sessions.get(key);
    if (!value) {
      let shared = credentials.get(token);
      if (!shared) {
        shared = modulePromise.then(async (module) => {
          const codec = new Codec(await WebAssembly.instantiate(module));

          const definitions = new Map<string, StoredDefinition>();
          const seedDefinitions=new Map<string,SeedDefinition>(),seedMetadata=new Map<string,Metadata>();
          if (storage) {
           let restored:TrunkSnapshot|undefined;
           try {
            const snapshot=(await storage(token,{kind:"trunk-read"})).trunk;
            if(snapshot){await prepareTrunk(snapshot,seedDefinitions,openRegistry);restored=snapshot;for(const m of snapshot.definitions)seedMetadata.set(m.digest,m);}
           } catch {
            // A replica's saved registry is a cache. An unreadable snapshot
            // must not prevent fetching the authoritative registry below.
            for(const definition of seedDefinitions.values())definition.dispose();
            seedDefinitions.clear();seedMetadata.clear();
           }
           if(restored)await storage(token,{kind:'trunk-stage',snapshot:restored});
          }
          const views = new ViewCache(fetcher);
          const summaries = new SummaryReplication(async (query, signal) => {
            const view = await views.query(query, token, signal, codec, definitions, storage, true);
            if (view.result.page.length || view.batch.rows.length) throw Error('Summary collection returned events');
            return view.result.analytics?.days.length ?? 0;
          });
          return { codec, definitions, views, summaries, seedDefinitions, seedMetadata };
        });
        credentials.set(token, shared);
        shared.catch(() => credentials.delete(token));
        if (credentials.size > 8)
          credentials.delete(credentials.keys().next().value!);
      }
      value = shared.then((shared) => ({
        ...shared,
        bindings: [],
        negotiation: "",
        native: false,
        trunks:new Map(),
      }));
      sessions.set(key, value);
      value.catch(() => sessions.delete(key));
    } else {
      sessions.delete(key);
      sessions.set(key, value);
    }
    if (sessions.size > 8) sessions.delete(sessions.keys().next().value!);
    return value;
  }
  async function negotiate(
    s: Session,
    token: string,
    storage = archive,
  ) {
    const known=s.views.catalog;
    if(known){s.bindings=known.bindings;s.native=true;s.negotiation=known.version;return;}
    if (s.negotiation) return;
    if (!s.loading)
      s.loading = (async () => {
        const request=createAbortScope([],30000);
        try {
          const snapshot=await readTrunk({runtime:controlRuntime,token,signal:request.signal,codecs:s.seedDefinitions,open:openRegistry,fetcher});
          for(const m of snapshot.definitions)s.seedMetadata.set(m.digest,m);
          if(storage)await storage(token,{kind:'trunk-stage',snapshot});
          s.bindings=snapshot.bindings;s.native=true;s.negotiation=snapshot.version;s.views.setTrunk(snapshot);
          if(storage)await storage(token,{kind:'trunk-save',snapshot});
        } finally {request.dispose();}
      })().finally(() => (s.loading = undefined));
    await s.loading;
  }
  function metadata(s: Session): Metadata[] {
    return [...new Set(s.bindings.map((b) => b.digest))].map((d) => {
      return s.seedMetadata.get(d)??s.definitions.get(d)!;
    });
  }
  function connect(port: MessagePort, storage = archive) {
    const archive = storage;
    const storedCatalogs = new Set<string>();
    const catalogKey = (token: string, b: Binding) =>
      JSON.stringify([token, b.tap, b.digest]);
    const jobs = new Map<
      number,
      { abort: AbortController; ack?: () => void }
    >();
    const send = (reply: Reply, transfer: Transferable[] = []) =>
      port.postMessage(reply, transfer);
    const cancel = (id: number) => {
      const job = jobs.get(id);
      job?.abort.abort();
      job?.ack?.();
      jobs.delete(id);
    };
    async function nativePages(id:number,token:string,selections:NativeSelection[],signal:AbortSignal){
      if(!archive)throw Error("Local native storage is unavailable");
      const cached=await session("",token,archive);
      const sources=new Map<string,NonNullable<Awaited<ReturnType<Archive>>["nativeDependencies"]>>();
      for(const selection of selections){
        if(sources.has(selection.source))continue;
        signal.throwIfAborted();
        const deps=(await archive(token,{kind:"native-dependencies",source:selection.source})).nativeDependencies;
        if(!deps)throw Error("Missing native source dependencies");
        sources.set(selection.source,deps);
        if(!cached.definitions.has(deps.definition.digest))cached.definitions.set(deps.definition.digest,{
          ...deps.definition,handle:cached.codec.define(deps.definition.pil),definition:decodeObject(deps.definition.pil) as PILDefinition,
        });
      }
      const bindings=[...sources.values()].map((s,stream)=>({stream,digest:s.definition.digest,tap:s.source.tap}));
      send({id,kind:"schemas",definitions:[...sources.values()].map(s=>s.definition),bindings});
      send({id,kind:"connected",headers:[]});
      let stream=0;
      for(const [key,deps] of sources){
        send({id,kind:"native-source",source:key,definition:deps.definition,binding:bindings[stream++],trie:deps.trie});
      }
      for(const selection of selections){
        let after:number|undefined;
        for(;;){
          signal.throwIfAborted();
          const page=(await archive(token,{kind:"native-page",source:selection.source,from:selection.from,to:selection.to,after})).nativePage;
          if(!page||page.rows.length%INDEX_ROW_BYTES||page.bases.length!==page.rows.length/INDEX_ROW_BYTES||page.offsets.length!==page.rows.length/INDEX_ROW_BYTES*2)throw Error("Invalid native page");
          const definition=cached.definitions.get(sources.get(selection.source)!.definition.digest)!;
          // Validate original payload frames without filling UUID/shared
          // positions or allocating expanded field-value objects.
          cached.codec.upload(page.payload);
          let end=0;
          for(let i=0;i<page.offsets.length;i+=2){
            const start=page.offsets[i],next=page.offsets[i+1];
            if(start!==end||next<=start||next>page.payload.length||cached.codec.scan(definition.handle,start,next-start)!==next-start)
              throw Error("Invalid native payload frame");
            end=next;
          }
          if(end!==page.payload.length)throw Error("Trailing native payload bytes");
          if(page.rows.length){
            const job=jobs.get(id);if(!job)signal.throwIfAborted();
            const ack=new Promise<void>(resolve=>job!.ack=resolve);
            send({id,kind:"native-batch",source:selection.source,page,where:selection.where},[
              page.rows.buffer as ArrayBuffer,page.payload.buffer as ArrayBuffer,page.offsets.buffer as ArrayBuffer,
            ]);
            await ack;if(job)job.ack=undefined;
          }
          if(page.done)break;
          if(page.after===undefined||after!==undefined&&page.after<=after)throw Error("Native page did not advance");
          after=page.after;
        }
      }
    }
    port.onmessage = (event: MessageEvent<Request>) => {
      const request = event.data;
      if(request.kind==='view'&&request.dispatch){request.dispatch.serviceReceivedAt=performance.timeOrigin+performance.now();request.dispatch.serviceJobs=jobs.size;}
      if (request.kind === "close") {
        for (const id of jobs.keys()) cancel(id);
        archive?.close?.();
        port.close();
        return;
      }
      if (request.kind === "cancel") {
        cancel(request.id);
        return;
      }
      if (request.kind === "ack") {
        jobs.get(request.id)?.ack?.();
        return;
      }
      if (jobs.has(request.id)) {
        send({
          id: request.id,
          kind: "error",
          message: "Duplicate worker request",
        });
        return;
      }
      const job = {
        abort: new AbortController(),
        ack: undefined as (() => void) | undefined,
      };
      jobs.set(request.id, job);
      const { signal } = job.abort,
        id = request.id;
      void (async () => {
        const token = request.token.trim();
        if(request.kind==='query-event'){
          const cached=await session('',token,archive),catalog=cached.views.catalog;
          if(!catalog)throw Error('Query telemetry requires a registered trunk');
          const cache=sourceScope(request.event.source).cache;
          const target=catalog.bindings.find(b=>(!cache||b.tap.startsWith(cache+'/'))&&b.tap.endsWith('/trie-trunk/queries/query'));
          if(!target?.branchId)throw Error('Query telemetry resource is not registered');
          const body=(await seedRuntime()).queryEvent(request.event);
          const response=await fetcher(resourceURL('seeds',target.tap),{method:'POST',signal,headers:{'Content-Type':'application/pil','Trunk-Branch':String(target.branchId),...(token?{Authorization:`Bearer ${token}`}:{})},body});
          if(response.status!==204)throw Error(`Query telemetry ingestion failed (${response.status}): ${await response.text()}`);
          return;
        }
        if(request.kind==='retry-storage'){if(archive)await archive(token,{kind:'replica-retry'});return;}
        if(request.kind==='time-window'){
          const cached=await session('',token,archive);
          const result=await time.collect(request.query,token,signal,archive,fetcher,result=>{if(!signal.aborted)send({id,kind:'time-window',result})});
          signal.throwIfAborted();
          if(!archive)throw Error('Calendar requires local timeline access');
          const q=request.query,plan=await prepareCalendarPlan(q.trunk,q.scope,q.from,q.to,token,archive,signal);
          const calendar=crypto.randomUUID();cached.views.setCalendar(calendar,plan,archive,token);
          send({id,kind:'time-window',result:{...result,calendar}});return;
        }
        if(request.kind==='trunk'){
          const cached=await session('',token,archive),scope=request.folder??'';
          let metadata=cached.trunks.get(scope);
          if(!metadata){
            metadata=new TrunkMetadata({
              restore:async()=>scope?undefined:(await archive?.(token,{kind:'trunk-read'}))?.trunk,
              refresh:(previous,signal)=>readTrunk({runtime:controlRuntime,token,signal,codecs:cached.seedDefinitions,open:openRegistry,fetcher,folder:scope,previous}),
              accept:async snapshot=>{
                await prepareTrunk(snapshot,cached.seedDefinitions,openRegistry);
                for(const m of snapshot.definitions)cached.seedMetadata.set(m.digest,m);
                if(!scope){if(archive)await archive(token,{kind:'trunk-stage',snapshot});cached.views.setTrunk(snapshot);cached.bindings=snapshot.bindings;cached.native=true;cached.negotiation=snapshot.version;}
              },
              publish:async snapshot=>{if(archive&&!scope)await archive(token,{kind:'trunk-save',snapshot});},
              publicationError:error=>send({id:0,kind:'storage-status',message:`Metadata is available in memory. Local storage could not be saved: ${error instanceof Error?error.message:String(error)}`}),
            });cached.trunks.set(scope,metadata);
          }
          await metadata.load(signal,snapshot=>{const {files,...trunk}=snapshot;send({id,kind:'trunk',trunk});});return;
        }
        if(request.kind==="persist-view"){
          if(!archive)return;
          const cached=await session("",token,archive);
          signal.throwIfAborted();
          await archive(token,{kind:"remember",definitions:request.definitions,bindings:request.bindings});
          await cached.views.persistHours(async hour=>{
            signal.throwIfAborted();
            await collectNative({metadata:cached.seedMetadata.get(request.bindings.find(b=>b.tap===hour.tap)?.digest??""),tap:hour.tap,definitionVersion:request.bindings.find(b=>b.tap===hour.tap)?.digest,from:new Date(hour.from).toISOString(),to:new Date(hour.to).toISOString(),token,signal,codec:cached.codec,definitions:cached.definitions,archive,fetcher});
          });
          return;
        }
        if(request.kind === "remember-view") {
          const cached=await session("",token,archive);
          signal.throwIfAborted();
          // Session storage already contains validated full PIL pages. Seed the
          // worker's bounded cache so later searches can reuse complete buckets.
          cached.views.remember(request.view);
          return;
        }
        if(request.kind === "prime-summaries") {
          const cached = await session("", token, archive);
          await cached.summaries.collect(request.service, Date.now(), request.recent, signal,
            progress => { if (!signal.aborted) send({id,kind:"summary-progress",progress}); });
          return;
        }
        if(request.kind === 'fast-dashboard') {
          validateFastRequest(request.request);
          const runtime=await seedRuntime(),summaryWindow=runtime.summaryWindow(request.request.from,request.request.to);
          const projection=new FastProjection(request.request,runtime,summaryWindow);
          let represented=0;
          try {
          const cached=await session('',token,archive);
          for(const scope of request.request.scopes) {
            const tap=scope+(scope.split('/').length===2?'/*/*':'/*');
            await time.collect({trunk:cached.views.catalog!,scope:tap,from:summaryWindow.from,to:summaryWindow.to,refresh:true},token,signal,archive,fetcher);
            for(let from=summaryWindow.from;from<summaryWindow.to;from+=7*dayMs) {
              signal.throwIfAborted();
              const to=Math.min(summaryWindow.to,from+7*dayMs);
              const calendar=crypto.randomUUID();cached.views.setCalendar(calendar,await prepareCalendarPlan(cached.views.catalog!,tap,from,to,token,archive!,signal),archive,token);
              const view=await cached.views.query({calendar,tap:scope+(scope.split('/').length===2?'/*/*':'/*'),from:new Date(from).toISOString(),to:new Date(to).toISOString(),include:'summary',size:1},token,signal,cached.codec,cached.definitions,archive,false,
                (manifest,days)=>projection.consume(manifest,days));
              if(!view.result.summary)throw Error('Missing summary coverage.');
              represented+=view.result.summary.count;
            }
          }
          signal.throwIfAborted();
          const result=projection.finish();projection.cover(represented,result.totals.events);
          send({id,kind:'fast-dashboard',result:{...result,requested:summaryWindow.requested}});
          }finally{projection.dispose();}
          return;
        }
        if(request.kind === "view") {
          const boundary:WorkerBoundary|undefined=request.boundaryReceived===undefined?undefined:{started:request.boundaryReceived,...(request.dispatch?{dispatch:request.dispatch}:{}),...(request.traceBoundary?{trace:{times:{},counts:{}}}:{})};
          const background:Promise<void>[]=[];
          const cached = await session("", token, archive);
          if(request.dispatch)request.dispatch.sessionReadyAt=performance.timeOrigin+performance.now();
          // Registered execution finishes its page before returning. Deliver it
          // with completion once; independent dataset updates can still stream.
          const terminal=request.query.engine!=='tantivy'&&!!cached.views.catalog?.definitions.every(d=>d.registered);
          const progressive=request.progressive&&!terminal;
          const view = await cached.views.query(request.query, token, signal, cached.codec, cached.definitions, archive, false, undefined, progressive ? page => {
            signal.throwIfAborted();
            send({id,kind:"view-page",view:page},[page.batch.bytes,page.batch.rows.buffer as ArrayBuffer]);
          } : undefined, request.progressive ? (summary,error)=>{
            if(!signal.aborted)send({id,kind:"view-dataset",summary,error});
          } : undefined,task=>background.push(task),boundary);
          signal.throwIfAborted();
          const boundaryEnd=boundary?performance.now():0;
          if(request.dispatch)request.dispatch.queryFinishedAt=performance.timeOrigin+boundaryEnd;
          const measured=boundary?{boundaryWorkerMs:boundaryEnd-boundary.started,boundarySteps:workerSteps(boundary,boundaryEnd)}:{};
          if(progressive)send({id,kind:"view-summary",result:view.result,...measured});
          else send({id,kind:"view",view,...measured},[view.batch.bytes,view.batch.rows.buffer as ArrayBuffer]);
          // The result is complete. Keep independent dataset and timeline maintenance tasks alive.
          await Promise.allSettled(background);
          return;
        }
        if(request.kind==="rebase"){
          if(!archive)throw Error("Local native storage is unavailable");
          await archive(token,{kind:"native-rebase",sources:request.sources});
          const shared=credentials.get(token);if(shared)(await shared).views.resetEvents();
          return;
        }
        if (request.kind === "begin") {
          if (!archive) throw Error("Local event storage is unavailable");
          await archive(token, {
            kind: "begin",
            collection: request.collection,
          });
          return;
        }
        if (request.kind === "restore") {
          if (!archive) return;
          const restored = await archive(token, {
            kind: "restore",
            collection: request.collection,
          });
          if (!restored.collection) return;
          const cursor = restored.cursor!;
          try {
            signal.throwIfAborted();
            send({
              id,
              kind: "collection",
              collection: restored.collection,
              coverage: restored.coverage ?? [],
            });
            send({
              id,
              kind: "schemas",
              definitions: restored.definitions!,
              bindings: restored.bindings!,
            });
            send({ id, kind: "connected", headers: [] });
            for (const binding of restored.bindings!)
              send({ id, kind: "binding", binding });
            // Reuse persisted definitions in later schema negotiation too.
            const cached = await session("", token, archive);
            for (const metadata of restored.definitions!)
              if (!cached.definitions.has(metadata.digest))
                cached.definitions.set(metadata.digest, {
                  ...metadata,
                  handle: cached.codec.define(metadata.pil),
                  definition: decodeObject(metadata.pil) as PILDefinition,
                });
            for (;;) {
              signal.throwIfAborted();
              const next = await archive(token, { kind: "next", cursor });
              signal.throwIfAborted();
              if (next.batch?.rows.length) {
                const ack = new Promise<void>((resolve) => (job.ack = resolve));
                send({ id, kind: "batch", batch: next.batch }, [
                  next.batch.bytes,
                  next.batch.rows.buffer as ArrayBuffer,
                ]);
                await ack;
                job.ack = undefined;
              }
              if (next.done) break;
            }
          } finally {
            await archive(token, { kind: "release", cursor });
          }
          if(restored.nativeSelections?.length)
            await nativePages(id,token,restored.nativeSelections,signal);
          return;
        }
        if (request.kind === "history") {
          const cached = await session("", token, archive);
          const response = await fetcher(historyURL(request.query), {
            method: "GET", signal, cache: "no-store",
            headers: { ...headers(token), "Definition-Known": [...cached.definitions.keys()].slice(-128).join(",") },
          });
          await checkResponse(response);
          const version = response.headers.get("Definition-Version");
          if (!version || !/^[a-f0-9]{64}$/.test(version) || response.headers.get("Trunk-Replay") !== "retained-history") {
            await response.body?.cancel();
            throw Error("Object history was not negotiated");
          }
          const state = observedState(request.query, response.headers.get("Trunk-State") ?? "");
          const bindings: Binding[] = [];
          const emitted = new Set<string>();
          send({ id, kind: "connected", headers: [] });
          await consume(response, new PILStream(cached.codec, cached.definitions,
            metadata => { emitted.add(metadata.digest); send({ id, kind: "definition", definition: metadata }); },
            binding => {
              if (binding.tap !== state.source) throw Error("History escaped the selected collection");
              if (!emitted.has(binding.digest)) {
                const { digest, store, pil } = cached.definitions.get(binding.digest)!;
                send({ id, kind: "definition", definition: { digest, store, pil } }); emitted.add(digest);
              }
              bindings.push(binding); send({ id, kind: "binding", binding });
            },
            async batch => {
              signal.throwIfAborted();
              const ack = new Promise<void>(resolve => job.ack = resolve);
              send({ id, kind: "batch", batch }, [batch.bytes, batch.rows.buffer as ArrayBuffer]);
              await ack; job.ack = undefined;
            }), signal);
          if (archive) await archive(token, { kind: "remember", bindings,
            definitions: [...new Set(bindings.map(b => b.digest))].map(digest => {
              const { store, pil } = cached.definitions.get(digest)!; return { digest, store, pil };
            }) });
          send({ id, kind: "history-state", state });
          return;
        }
        const tap =
          request.kind === "schemas" ? request.tap : request.selection.tap;
        const s = await session(tap, token, archive);
        if (request.kind === "schemas") {
          await negotiate(s, token, archive);
          signal.throwIfAborted();
          send({
            id,
            kind: "schemas",
            definitions: metadata(s),
            bindings: s.bindings,
          });
          return;
        }
        let nativeTail:Selection[]|undefined;
        const where=conditions(request.selection);
        if(archive&&request.collection&&s.native&&where&&request.selection.to&&
          !request.selection.follow&&!request.selection.tail&&request.selection.limit===null){
          nativeTail=[];
          const sourceGroups=await searchTasks(s.bindings.filter(b=>matchesSource(request.selection.tap,b.tap)),signal,async(binding,_i,branchSignal)=>collectNative({metadata:s.seedMetadata.get(binding.digest),
            onProvisional:(tap,from,to)=>{
              from=Math.max(from,Date.parse(request.selection.from));to=Math.min(to,Date.parse(request.selection.to!));
              if(from<to&&!nativeTail!.some(s=>s.tap===tap&&Date.parse(s.from)===from&&Date.parse(s.to!)===to))nativeTail!.push({...request.selection,tap,from:new Date(from).toISOString(),to:new Date(to).toISOString()});
            },
            tap:binding.tap,from:request.selection.from,to:request.selection.to!,
            token,signal:branchSignal,codec:s.codec,definitions:s.definitions,archive,fetcher,
            definitionVersion:binding.digest,
            collection:request.collection,selection:request.selection,
          }));
          const sources=[...new Set(sourceGroups.flat())];
          await nativePages(id,token,sources.map(source=>({source,
            from:Date.parse(request.selection.from),to:Date.parse(request.selection.to!),where,
          })),signal);
          if(!nativeTail.length)return;
        }
        signal.throwIfAborted();
        send({
          id,
          kind: "schemas",
          definitions: metadata(s),
          bindings: s.bindings,
        });
        const sentDefinitions = new Set(metadata(s).map((m) => m.digest));
        const started = Date.now();
        let selections = nativeTail??[request.selection];
        let planned = false;
        if (archive && request.collection && !nativeTail) {
          const local = await archive(token, {
            kind: "lookup",
            collection: request.collection,
            selection: request.selection,
            started,
          });
          if (local.cursor && local.gaps) {
            planned = true;
            const cursor = local.cursor;
            try {
              signal.throwIfAborted();
              for (const m of local.definitions!) {
                if (!s.definitions.has(m.digest))
                  s.definitions.set(m.digest, {
                    ...m,
                    handle: s.codec.define(m.pil),
                    definition: decodeObject(m.pil) as PILDefinition,
                  });
                send({ id, kind: "definition", definition: m });
                sentDefinitions.add(m.digest);
              }
              send({
                id,
                kind: "schemas",
                definitions: local.definitions!,
                bindings: local.bindings!,
              });
              send({ id, kind: "connected", headers: [] });
              for (const binding of local.bindings!)
                send({ id, kind: "binding", binding });
              for (;;) {
                signal.throwIfAborted();
                const next = await archive(token, { kind: "next", cursor });
                signal.throwIfAborted();
                if (next.batch?.rows.length) {
                  const ack = new Promise<void>(
                    (resolve) => (job.ack = resolve),
                  );
                  send({ id, kind: "batch", batch: next.batch }, [
                    next.batch.bytes,
                    next.batch.rows.buffer as ArrayBuffer,
                  ]);
                  await ack;
                  job.ack = undefined;
                }
                if (next.done) break;
              }
              selections = local.gaps.map((gap) => ({
                ...request.selection,
                from: new Date(gap.from).toISOString(),
                to: new Date(gap.to).toISOString(),
              }));
            } finally {
              await archive(token, { kind: "release", cursor });
            }
          }
        }
        for (const selection of selections) {
          signal.throwIfAborted();
          const response = await fetcher(collectionURL(selection), {
            method: "GET",
            headers: {
              ...headers(token),
              "Definition-Version": s.negotiation,
              "Definition-Known": [...s.definitions.keys()]
                .slice(-128)
                .join(","),
            },
            signal,
            cache: "no-store",
          });
          await checkResponse(response);
          const version = response.headers.get("Definition-Version");
          if (!version || !/^[a-f0-9]{64}$/.test(version))
            throw Error("Missing definition version");
          send({
            id,
            kind: "connected",
            headers: (response.headers.get("Trunk-Entry-Headers") ?? "")
              .split(",")
              .map((s) => s.trim())
              .filter(Boolean),
          });
          const bindings: Binding[] = [];
          const activeBindings = new Map<number, Binding>();
          await consume(
            response,
            new PILStream(
              s.codec,
              s.definitions,
              (m) => {
                sentDefinitions.add(m.digest);
                send({ id, kind: "definition", definition: m });
              },
              (binding) => {
                if (!sentDefinitions.has(binding.digest)) {
                  const { digest, store, pil } = s.definitions.get(
                    binding.digest,
                  )!;
                  send({
                    id,
                    kind: "definition",
                    definition: { digest, store, pil },
                  });
                  sentDefinitions.add(binding.digest);
                }
                bindings.push(binding);
                activeBindings.set(binding.stream, binding);
                if (
                  !s.bindings.some(
                    (b) => b.tap === binding.tap && b.digest === binding.digest,
                  )
                )
                  s.bindings.push(binding);
                send({ id, kind: "binding", binding });
              },
              async (batch) => {
                signal.throwIfAborted();
                if (archive && request.collection && !nativeTail) {
                  const used = [
                    ...new Set(
                      Array.from(batch.rows).filter((_, i) => i % 3 === 0),
                    ),
                  ].map((stream) => activeBindings.get(stream)!);
                  const definitions = [
                    ...new Set(
                      used
                        .filter(
                          (b) => !storedCatalogs.has(catalogKey(token, b)),
                        )
                        .map((b) => b.digest),
                    ),
                  ].map((d) => {
                    const { digest, store, pil } = s.definitions.get(d)!;
                    return { digest, store, pil };
                  });
                  const saved = await archive(token, {
                    kind: "save",
                    collection: request.collection,
                    definitions,
                    bindings: used,
                    batch,
                  });
                  for (const b of used)
                    storedCatalogs.add(catalogKey(token, b));
                  batch = saved.batch!;
                  signal.throwIfAborted();
                }
                const ack = new Promise<void>((resolve) => (job.ack = resolve));
                send({ id, kind: "batch", batch }, [
                  batch.bytes,
                  batch.rows.buffer as ArrayBuffer,
                ]);
                await ack;
                job.ack = undefined;
                signal.throwIfAborted();
              },
            ),
            signal,
          );
          if (archive && request.collection && !nativeTail)
            await archive(token, {
              kind: "finish",
              collection: request.collection,
              selection,
              started,
            });
          // Only a fully received response can certify this cached version. If a
          // follow is canceled midway, its definitions remain reusable but the next
          // collect conservatively negotiates again when the catalog has changed.
          if (version !== s.negotiation)
            s.bindings = [
              ...new Map(
                bindings.map((b) => [JSON.stringify([b.tap, b.digest]), b]),
              ).values(),
            ];
          if (archive)
            await archive(token, {
              kind: "remember",
              definitions: metadata(s),
              bindings: s.bindings,
            });
          s.negotiation = version;
          send({
            id,
            kind: "schemas",
            definitions: metadata(s),
            bindings: s.bindings,
          });
        }
        // Every gap completed and every reused interval was already certified.
        // This also gives a cache-only collection its own completion metadata.
        if (planned && archive && request.collection) {
          signal.throwIfAborted();
          await archive(token, {
            kind: "finish",
            collection: request.collection,
            selection: request.selection,
            started,
          });
        }
      })()
        .then(
          () => {
            if (!signal.aborted) send({ id, kind: "done" });
          },
          (error) => {
            if (!signal.aborted)
              send({
                id,
                kind: "error",
                cacheIssue: nativeCacheIssue(error),
                message: error instanceof Error ? error.message : String(error),
              });
          },
        )
        .finally(() => {
          job.abort.abort();
          if (jobs.get(id) === job) jobs.delete(id);
        });
    };
    port.start();
  }

  return connect;
}
