/** Host objects and lifetime management only. Rust owns filtering and decoding. */
export async function createSeeds(wasm = new URL('./data-seeds.wasm', import.meta.url), {files,residentBytes} = {}) {
  const decoder = new TextDecoder(), encoder = new TextEncoder();
  let api, frames = [], result, names = [], missing = [], makeView;
  let storageError;
  const safeOffset = n => {if(n<0n||n>BigInt(Number.MAX_SAFE_INTEGER))throw RangeError('Storage offset exceeds host precision');return Number(n);};
  const hostFile = id => {if(!files)throw Error('No storage adapter configured');return files(safeOffset(id));};
  const io = action => {try{return action();}catch(error){storageError=error;return -1;}};
  const text = (p, n) => decoder.decode(new Uint8Array(api.memory.buffer, p, n));
  function put(value) {
    if (!frames.length) { result = value; return; }
    const f = frames[frames.length - 1];
    if (Array.isArray(f.value)) f.value.push(value);
    else if (f.value instanceof Map) {
      if (f.hasKey) { f.value.set(f.key, value); f.hasKey = false; }
      else { f.key = value; f.hasKey = true; }
    } else if(f.known && f.key!=='__proto__') f.value[f.key]=value;
    else Object.defineProperty(f.value, f.key, { value, enumerable: true, writable: true, configurable: true });
  }
  const imports = { seed_host: {
    absent() { put(undefined); }, null() { put(null); }, boolean(v) { put(v !== 0); }, number(v) { put(v); },
    signed(v) { put(v); }, unsigned(v) { put(BigInt.asUintN(64, v)); },
    big_integer(p,n) { put(BigInt(text(p,n))); },
    text(p,n) { put(text(p,n)); }, bytes(p,n) { put(new Uint8Array(api.memory.buffer,p,n).slice()); },
    array() { frames.push({ value: [] }); }, object() { frames.push({ value: {} }); },
    map() { frames.push({ value: new Map(), hasKey: false }); },
    record() { frames.push({value:{},known:true}); }, key(p,n) { frames.at(-1).key = text(p,n); },
    field(i) { frames.at(-1).key = names[i]; }, end() { put(frames.pop().value); },
    shape() { names = []; }, name(p,n) { names.push(text(p,n)); }, missing(i) { missing.push(i); },
    view(row) { put(makeView(row,names,missing)); missing = []; },
  }, seed_files: {
    read(id,at,p,n) {return io(()=>{const count=hostFile(id).read(new Uint8Array(api.memory.buffer,p,n),{at:safeOffset(at)});if(!Number.isInteger(count)||count<0||count>n)throw Error('Invalid host read count');return count;});},
    write(id,at,p,n) {return io(()=>{const count=hostFile(id).write(new Uint8Array(api.memory.buffer,p,n),{at:safeOffset(at)});if(!Number.isInteger(count)||count<0||count>n)throw Error('Invalid host write count');return count;});},
    resize(id,length) {return io(()=>{hostFile(id).truncate(safeOffset(length));return 0;});},
    flush(id) {return io(()=>{hostFile(id).flush();return 0;});},
  } };
  const source = wasm instanceof URL || typeof wasm === 'string' ? await (await fetch(wasm)).arrayBuffer() : wasm;
  const instance = await WebAssembly.instantiate(source, imports);
  api = instance.instance?.exports ?? instance.exports;
  const check = value => { if ((value >>> 0) === 0xffffffff) {const error=storageError;storageError=undefined;throw error??Error(text(api.seed_error_ptr(),api.seed_error_len()));} return value >>> 0; };
  if(residentBytes!==undefined) {
    if(!Number.isSafeInteger(residentBytes)||residentBytes<1||residentBytes>0xffffffff)throw RangeError('Invalid resident byte budget');
    check(api.seed_resident_budget(residentBytes));
  }
  function input(bytes, suffix) {
    const reader = bytes && typeof bytes.read === 'function' ? bytes : undefined;
    const data = reader ? undefined : ArrayBuffer.isView(bytes) ? new Uint8Array(bytes.buffer,bytes.byteOffset,bytes.byteLength) : new Uint8Array(bytes);
    const length = reader ? reader.byteLength : data.byteLength;
    const size=length+(suffix?.byteLength??0);
    if (!Number.isSafeInteger(length) || length<0 || !Number.isSafeInteger(size) || size>0xffffffff) throw RangeError('Invalid Seed input length');
    const p = api.seed_input(size);
    if (!p && size) throw Error(text(api.seed_error_ptr(),api.seed_error_len()));
    const target = new Uint8Array(api.memory.buffer,p,length);
    if(reader) {
      // The host only performs I/O. Rust takes ownership of this allocation
      // when opening the Pack; no intermediate host payload is constructed.
      if(reader.read(target)!==length) throw Error('Short Seed input read');
    } else target.set(data);
    if(suffix)new Uint8Array(api.memory.buffer,p+length,suffix.byteLength).set(suffix);
    return length;
  }
  function capture(fn, factory) {
    frames = []; result = undefined; missing = []; makeView = factory;
    try { check(fn()); if (frames.length) throw Error('Unfinished host value'); return result; }
    finally { frames = []; result = undefined; makeView = undefined; missing = []; }
  }
  const finalizers = new FinalizationRegistry(([kind,id]) => api.seed_release(kind,id));
  class Handle {
    constructor(kind,id) { this.kind=kind; this.id=check(id); this.disposed=false; finalizers.register(this,[kind,this.id],this); }
    use() { if (this.disposed) throw Error('Released Seed handle'); return this.id; }
    dispose() { if (!this.disposed) { api.seed_release(this.kind,this.id); finalizers.unregister(this); this.disposed=true; } }
  }
  class Batch extends Handle {
    constructor(id) { super(4,id); this.length=check(api.seed_batch_len(this.id)); }
    get retainedBytes() { return check(api.seed_retained_bytes(this.use())); }
    objects() { return capture(() => api.seed_results(this.use(),0)); }
    origins() { return capture(() => api.seed_batch_origins(this.use())); }
    views() {
      const batch=this;
      // One property plan per shape/presence pattern. Creating a view does not
      // install a descriptor or allocate a cached value for every field.
      const shapes=new WeakMap(), rows=new WeakMap();
      const read=(target,name) => {
        const state=rows.get(target),i=state.plan.ordinals.get(name);
        return i===undefined ? Reflect.get(Object.prototype,name) : capture(() => api.seed_field(batch.use(),state.row,i));
      };
      const handler={
        get:read,
        has(target,name) {return rows.get(target).plan.ordinals.has(name) || name in Object.prototype;},
        ownKeys(target) {return rows.get(target).plan.names;},
        getOwnPropertyDescriptor(target,name) {
          if (!rows.get(target).plan.ordinals.has(name)) return undefined;
          return {enumerable:true,configurable:true,get(){return read(target,name);}};
        },
        set(){return false;},defineProperty(){return false;},deleteProperty(){return false;},preventExtensions(){return false;},setPrototypeOf(){return false;},
      };
      return capture(() => api.seed_results(batch.use(),1),(row,fields,absent) => {
        let patterns=shapes.get(fields);
        if (!patterns) {patterns=new Map();shapes.set(fields,patterns);}
        const shapeKey=absent.join(',');
        let plan=patterns.get(shapeKey);
        if (!plan) {
          const omitted=new Set(absent),ordinals=new Map();
          fields.forEach((name,i) => {if(!omitted.has(i))ordinals.set(name,i);});
          plan={ordinals,names:[...ordinals.keys()]};patterns.set(shapeKey,plan);
        }
        const target={};rows.set(target,{row,plan});return new Proxy(target,handler);
      });
    }
  }
  class Pack extends Handle {
    constructor(id) { super(1,id); }
    retain() { return new Pack(api.seed_pack_retain(this.use())); }
    query(additional=[]) {return sourceQuery(this,additional);}
    info() { return capture(() => api.seed_pack_info(this.use())); }
    bytes() { return capture(() => api.seed_pack_bytes(this.use())); }
    withIndex(bytes, hour, leaves) {
      const id=this.use();
      if (typeof hour==='number' && !Number.isSafeInteger(hour)) throw RangeError('Invalid index hour');
      const time=BigInt(hour);if(time<0n || time>=(1n<<48n)) throw RangeError('Invalid index hour');
      if(leaves!==undefined && (!(leaves instanceof Leaves))) throw TypeError('Expected a state trie');
      const leafId=leaves?.use();
      input(bytes);return new Pack(leaves===undefined?api.seed_pack_index(id,time):api.seed_pack_leaf_index(id,time,leafId));
    }
  }
  class Leaves extends Handle {
    constructor(id) {super(6,id);}
    append(first,bytes) {
      if(!Number.isSafeInteger(first)||first<1||first>65536) throw RangeError('Invalid state batch ordinal');
      const id=this.use();input(bytes);check(api.seed_leaf_append(id,first));return this;
    }
  }
  function sourceQuery(first,additional) {
    first.use();const sources=Array.from(additional,source=>{
      if(!(source instanceof Reader)&&!(source instanceof Pack))throw TypeError('Expected a reader or Pack from this runtime');
      source.use();return source;
    });return new Query(first,sources);
  }
  class Query {
    constructor(store,readers) { this.store=store;this.readers=readers;this.spec={};this.cursor=null;this.disposed=false; }
    edit() { if (this.disposed || this.cursor) throw Error('Query has already started or was released'); }
    context(value) {this.edit();this.spec.context={...value};return this;}
    filter(field,op,value) { this.edit();this.spec.where=typeof field==='object'?field:{field,op,value};return this; }
    select(...fields) { this.edit();this.spec.select=fields.flat();return this; }
    cast(field,alias,cast) {this.edit();(this.spec.casts??=[]).push({field,alias,cast});return this;}
    version(major,minor) {this.edit();this.spec.version=[major,minor];return this;}
    orderBy(field,direction='asc') { this.edit();this.spec.orderBy={field,direction};return this; }
    limit(n) { this.edit();this.spec.limit=n;return this; }
    offset(n) { this.edit();this.spec.offset=n;return this; }
    start() {
      if (this.disposed) throw Error('Released query');
      if (!this.cursor) {
        this.spec.context={referenceMs:Date.now(),...this.spec.context};
        const store=this.store.use();
        const spec=this.readers?{...this.spec,readers:[this.store,...this.readers].map(reader=>({id:reader.use(),kind:reader.kind}))}:this.spec;
        input(encoder.encode(JSON.stringify(spec,(_,v)=>typeof v==='bigint'?{$integer:v.toString()}:v)));
        this.cursor=new Handle(3,this.readers?api.seed_query_readers():api.seed_query(store));
        this.readers=undefined;
      }
    }
    fetch(n=256) {
      if (!Number.isSafeInteger(n) || n<0 || n>0xffffffff) throw RangeError('Invalid fetch size');
      this.start();
      return new Batch(api.seed_fetch(this.cursor.use(),n));
    }
    stats() { return this.cursor ? capture(()=>api.seed_query_stats(this.cursor.use())) : {scanned:0,matched:0}; }
    poll(n=256,work=1024) {
      if(!Number.isSafeInteger(n)||n<1||n>4096||!Number.isSafeInteger(work)||work<1||work>0xffffffff)throw RangeError('Invalid query slice');
      this.start();
      const slice=capture(()=>api.seed_poll(this.cursor.use(),n,work));
      return {...slice,batch:slice.batch===null?null:new Batch(slice.batch)};
    }
    objects(n=256) { const batch=this.fetch(n);try {return batch.objects();} finally {batch.dispose();} }
    views(n=256) { return this.fetch(n).views(); }
    stalks({rows=1_000_000}={}) {
      this.edit();count32(rows);this.start();
      const growth=new Growth(api.seed_stalks(this.cursor.use(),rows));
      this.dispose();this.store=undefined;return growth;
    }
    cancel() { if (this.cursor) check(api.seed_cancel(this.cursor.use())); }
    dispose() { this.cursor?.dispose();this.readers=undefined;this.disposed=true; }
  }
  class Store extends Handle {
    constructor(id) { super(2,id ?? api.seed_store()); }
    pack(pack) { if (!(pack instanceof Handle) || pack.kind!==1) throw TypeError('Expected a Seed Pack');check(api.seed_store_pack(this.use(),pack.use()));return this; }
    query() { this.use();return new Query(this); }
    reconstruct(length,count) {return new Replica(api.seed_reconstruct(this.use(),BigInt(addressText(length)),BigInt(addressText(count))));}
  }
  class Growth extends Handle {
    constructor(id){super(15,id);}
    poll(work=1024){if(!Number.isSafeInteger(work)||work<1||work>0xffffffff)throw RangeError('Invalid Stalk work budget');return capture(()=>api.seed_stalks_poll(this.use(),work));}
    finish(){const result=new Stalks(api.seed_stalks_finish(this.use()));this.dispose();return result;}
    cancel(){this.dispose();}
  }
  class Stalks extends Handle {
    constructor(id){super(16,id);}
    info(){return capture(()=>api.seed_stalks_info(this.use()));}
    sum(field){const id=this.use();input(encoder.encode(field));return capture(()=>api.seed_stalks_sum(id));}
    objects(first=0,count=256){count32(first);count32(count);return capture(()=>api.seed_stalks_objects(this.use(),first,count));}
  }
  function branchId(id) {
    if(!Number.isSafeInteger(id)||id<1||id>0xffff)throw RangeError('Invalid Branch ID');
    return id;
  }
  function addressText(address) {
    if(typeof address!=='bigint'&&(typeof address!=='number'||!Number.isSafeInteger(address)))throw RangeError('Invalid Pack address');
    const n=BigInt(address);if(n<0n||n>0xffffffffffffffffn)throw RangeError('Invalid Pack address');
    return n.toString();
  }
  function count32(n) {if(!Number.isInteger(n)||n<0||n>0xffffffff)throw RangeError('Invalid operation or memory limit');return n;}
  class Replica extends Handle {
    constructor(id) {super(9,id);}
    retain() {return new Replica(api.seed_replica_retain(this.use()));}
    publication(time,generation,placements,file) {
      if(!(time instanceof Timeline))throw TypeError('Expected this runtime’s Time trie');
      const id=this.use(),timeId=time.use();
      if(file!==undefined&&(!Number.isSafeInteger(file)||file<0))throw RangeError('Invalid Time file handle');
      input(encoder.encode(JSON.stringify({generation,file,placements:Array.from(placements,p=>
        [p.hour,p.start,p.end,p.packs,p.beds,p.count].map(addressText))})));
      return capture(()=>api.seed_replica_publication(id,timeId));
    }
    collect(time,ranges,window) {
      if(!(time instanceof Timeline))throw TypeError('Expected this runtime’s Time trie');
      const id=this.use(),timeId=time.use();
      input(encoder.encode(JSON.stringify({...window,ranges:Array.from(ranges)})));
      return new Collection(api.seed_collection(id,timeId));
    }
    batch(trunk,file,operations=1024) {
      if(!(trunk instanceof Handle)||trunk.kind!==7)throw TypeError('Expected this runtime’s Trunk');
      return new WritePlan(api.seed_replica_batch(this.use(),trunk.use(),BigInt(addressText(file)),count32(operations)));
    }
  }
  class Collection extends Handle {
    constructor(id) {super(11,id);}
    peek() {return capture(()=>api.seed_collection_peek(this.use()));}
    accept(pack) {
      if(!(pack instanceof Pack))throw TypeError('Expected this runtime’s Pack');
      return capture(()=>api.seed_collection_accept(this.use(),pack.use()));
    }
    load(batch,pack,selection) {
      if(!(batch instanceof WritePlan)||!(pack instanceof Pack))throw TypeError('Expected this runtime’s write plan and Pack');
      if(selection!==undefined) {
        if(!(selection.reader instanceof Reader))throw TypeError('Expected this runtime’s reader');
        return capture(()=>api.seed_collection_load_selected(this.use(),batch.use(),pack.use(),selection.reader.use(),count32(selection.ordinal)));
      }
      return capture(()=>api.seed_collection_load(this.use(),batch.use(),pack.use()));
    }
    finish() {return capture(()=>api.seed_collection_finish(this.use()));}
  }
  class WritePlan extends Handle {
    constructor(id) {super(10,id);}
    load(address,value,pack) {
      if(address instanceof Reader) {
        if(!(pack instanceof Pack))throw TypeError('Expected this runtime’s Pack');
        check(api.seed_replica_pack_selected(this.use(),address.use(),count32(value),pack.use()));return this;
      }
      const id=this.use(),at=BigInt(addressText(address));
      if(value instanceof Pack)check(api.seed_replica_pack(id,at,value.use()));
      else {input(value);check(api.seed_replica_load(id,at));}
      return this;
    }
    footer() {check(api.seed_replica_footer(this.use()));return this;}
    submit() {return BigInt(capture(()=>api.seed_write_submit(this.use())));}
  }
  class Reader extends Handle {
    constructor(id) {super(2,id);}
    ready(owner,source) {owner.call(9,source,0,this.use());return this;}
    prepare(first=0,count=16) {return capture(()=>api.seed_reader_prepare(this.use(),count32(first),count32(count)));}
    retain(ordinal,pack) {
      if(!(pack instanceof Pack))throw TypeError('Expected a Pack from this runtime');
      check(api.seed_reader_retain(this.use(),count32(ordinal),pack.use()));return this;
    }
    range(from,to) {return new Reader(api.seed_reader_range(this.use(),BigInt(addressText(from)),BigInt(addressText(to))));}
    ranges(periods) {const id=this.use();input(encoder.encode(JSON.stringify(Array.from(periods,([a,z])=>[addressText(a),addressText(z)]))));return new Reader(api.seed_reader_ranges(id));}
    index(file,length,time,hours,leaves=[],space='beds') {
      const id=this.use();
      if(!(time instanceof Timeline))throw TypeError('Expected a timeline from this runtime');
      const roots=Array.from(leaves,root=>{if(!(root instanceof Leaves))throw TypeError('Expected Leaves from this runtime');return root.use();});
      const timeId=time.use();
      input(encoder.encode(JSON.stringify({hours:Array.from(hours,addressText),leaves:roots,space})));
      check(api.seed_reader_index(id,BigInt(addressText(file)),BigInt(addressText(length)),timeId));
      return this;
    }
    query(additional=[]) {return sourceQuery(this,additional);}
    pack(ordinal) {return new Pack(api.seed_reader_pack(this.use(),count32(ordinal)));}
  }
  class Packs extends Handle {
    constructor(id) {super(2,id);}
    reader(file,length,addresses,maximum=64*1024*1024) {
      const id=this.use();input(encoder.encode(JSON.stringify(Array.from(addresses,addressText))));
      return new Reader(api.seed_reader(id,BigInt(addressText(file)),BigInt(addressText(length)),count32(maximum)));
    }
    strings(entries) {input(encoder.encode(JSON.stringify(entries)));check(api.seed_registered_strings(this.use()));}
    receiveStored(maximum) {return new Receiver(this,maximum,true,false,true);}
    receiveForWrite(maximum) {return new Receiver(this,maximum,true,true,true);}
    load(branch,bytes,maximum=0) {
      const id=this.use();branchId(branch);
      if(!Number.isSafeInteger(maximum)||maximum<0||maximum>0xffffffff)throw RangeError('Invalid Seed read budget');
      if(maximum && bytes.byteLength>maximum)throw RangeError('Seed Pack exceeds receive limit');
      input(bytes);check(api.seed_registered_store_load(id,branch,maximum));return this;
    }
    pack(branch,pack) {
      branchId(branch);
      if(!(pack instanceof Pack))throw TypeError('Expected a Seed Pack');
      check(api.seed_registered_store_pack(this.use(),branch,pack.use()));return this;
    }
    loadBox(branch,bytes,addresses) {
      const id=this.use();branchId(branch);
      const bindings=Array.from(addresses,addressText);
      input(bytes);check(api.seed_stage_box());
      try {input(encoder.encode(JSON.stringify(bindings)));check(api.seed_registered_store_box(id,branch));return this;}
      finally {api.seed_drop_box();}
    }
    query() {this.use();return new Query(this);}
    reconstruct(length,count) {return new Replica(api.seed_reconstruct(this.use(),BigInt(addressText(length)),BigInt(addressText(count))));}
  }
  class Branches {
    constructor(trunk,selected=null) {this.trunk=trunk;this.selected=selected;}
    select(ids) {this.trunk.use();return new Branches(this.trunk,Array.from(ids,branchId));}
    packs() {
      const id=this.trunk.use();input(encoder.encode(JSON.stringify(this.selected)));
      return new Packs(api.seed_trunk_packs(id));
    }
    beds() {return this.packs();}
    summaries() {
      if(this.selected?.length!==1)throw Error('Select one registered branch for its summaries');
      const trunk=this.trunk,branch=this.selected[0];let closed=false;
      const open=(bytes,first,pack)=>{
        if(closed)throw Error('Released summary scope');
        const id=trunk.use();input(bytes);
        return summaryReader((pack?api.seed_registered_summary_open:api.seed_registered_summary_box_open)(id,branch,BigInt(addressText(first))));
      };
      return {open:(bytes,first)=>open(bytes,first,false),pack:(bytes,first)=>open(bytes,first,true),dispose(){closed=true;}};
    }
    leaves() {
      this.trunk.use();
      if(this.selected?.length!==1)throw Error('Select one registered branch for its leaves');
      const branch=this.selected[0],trunk=this.trunk;
      return {
        read(bytes) {const id=trunk.use();input(bytes);return new Leaves(api.seed_trunk_leaf_read(id,branch));},
        inspect(bytes) {const id=trunk.use();input(bytes);return capture(()=>api.seed_trunk_leaf_inspect(id,branch));},
        labels(bytes) {const id=trunk.use();input(bytes);return capture(()=>api.seed_trunk_leaf_labels(id,branch));},
        label(bytes,leaf,text) {if(!Number.isInteger(leaf)||leaf<0||leaf>65535)throw RangeError('Invalid leaf');const id=trunk.use(),length=input(bytes,encoder.encode(text??''));return capture(()=>api.seed_trunk_leaf_label(id,branch,length,leaf));},
        definitions(bytes) {const id=trunk.use();input(bytes);return capture(()=>api.seed_trunk_leaf_definitions(id,branch));},
      };
    }
    time(bytes,received) {
      const id=this.trunk.use();
      if(this.selected?.length!==1)throw Error('Select one registered branch for its time trie');
      if(received!==undefined) {
        const length=input(bytes,encoder.encode(JSON.stringify(Array.from(received))));
        return new Timeline(api.seed_trunk_replica_time(id,this.selected[0],length));
      }
      input(bytes);
      return new Timeline(api.seed_trunk_time(id,this.selected[0]));
    }
  }
  class Timeline extends Handle {
    constructor(id) {super(8,id);}
    retain() {return new Timeline(api.seed_time_retain(this.use()));}
    info() {return capture(()=>api.seed_time_info(this.use()));}
    get(ms) {return capture(()=>api.seed_time_leaf(this.use(),BigInt(addressText(ms))));}
    range(from,to) {return capture(()=>api.seed_time_range(this.use(),BigInt(addressText(from)),BigInt(addressText(to))));}
  }
  class Receiver extends Handle {
    constructor(definition,maximum,stored=false,verifyWrite=false,registered=false) {
      if (!Number.isSafeInteger(maximum)||maximum<0||maximum>0xffffffff) throw RangeError('Invalid Seed Pack receive limit');
      super(5,registered?api.seed_registered_receive(definition.use(),maximum,verifyWrite):
        (verifyWrite?api.seed_receive_for_write:stored?api.seed_receive_stored:api.seed_receive)(definition.use(),maximum));
    }
    load(bytes) {
      const id=this.use();input(bytes);
      const count=check(api.seed_receive_load(id)),packs=[];
      try {for(let i=0;i<count;i++)packs.push(new Pack(api.seed_receive_next(id)));return packs;}
      catch(error) {for(const pack of packs)pack.dispose();throw error;}
    }
    feed(bytes) {const id=this.use();input(bytes);check(api.seed_receive_feed(id));}
    strings(entries) {input(encoder.encode(JSON.stringify(entries)));check(api.seed_receive_strings(this.use()));}
    step() {const state=check(api.seed_receive_step(this.use()));return {missing:state&4?capture(()=>api.seed_receive_missing(this.use())).map(Number):undefined,more:!!(state&2),pack:state&1?new Pack(api.seed_receive_next(this.use())):null};}
    finish() {return check(api.seed_receive_finish(this.use()));}
  }
  function store(bytes,definitions,addresses) {
    if(bytes===undefined)return new Store();
    const ids=definitions.map(d=>{if(!(d instanceof Handle)||d.kind!==0)throw TypeError('Expected registered Seed Definitions');return d.use();});
    let bindings;
    if(addresses!==undefined) {
      if(addresses.length!==ids.length)throw RangeError('Pack address and Definition counts differ');
      bindings=addresses.map((address,i)=>[ids[i],addressText(address)]);
    }
    input(bytes);check(api.seed_stage_box());
    try {input(encoder.encode(JSON.stringify(bindings??ids)));return new Store(bindings===undefined?api.seed_open_box():api.seed_open_box_addresses());}
    finally {api.seed_drop_box();}
  }
  function summaryReader(id) {
        const reader=new Handle(12,id);
        reader.info=()=>capture(()=>api.seed_summary_info(reader.use()));
        reader.rows=(from,to)=>capture(()=>api.seed_summary_rows(reader.use(),BigInt(addressText(from)),BigInt(addressText(to))));
        reader.query=(from,to)=>{
          const spec={from,to,measures:[],percentiles:[]};
          const query={
            span(milliseconds){spec.span=milliseconds;return query;},
            select(field,aggregate){spec.measures.push({field,aggregate});return query;},
            perc(field,aggregate,n){spec.percentiles.push({field,aggregate,n});return query;},
            execute(){const id=reader.use();input(encoder.encode(JSON.stringify({...spec,context:{referenceMs:Date.now(),...spec.context}})));return capture(()=>api.seed_summary_query(id));},
          };
          return query;
        };
        reader.preview=(from,to,where,context={})=>{const id=reader.use();input(encoder.encode(JSON.stringify({from,to,where,context:{referenceMs:Date.now(),...context}},(_,v)=>typeof v==='bigint'?{$integer:v.toString()}:v)));return capture(()=>api.seed_summary_query(id));};
        return reader;
  }
  function prepareDefinition(definition) {
      definition.analytics = () => capture(()=>api.seed_summary_analytics(definition.use()));
      definition.open = bytes => { definition.use();input(bytes);return new Pack(api.seed_open_pack(definition.id)); };
      definition.receive = maximum => new Receiver(definition,maximum);
      definition.receiveStored = maximum => new Receiver(definition,maximum,true);
      definition.receiveForWrite = maximum => new Receiver(definition,maximum,true,true);
      definition.openStored = (bytes,maximum=0) => {
        definition.use();
        if(!Number.isSafeInteger(maximum)||maximum<0||maximum>0xffffffff)throw RangeError('Invalid Seed read budget');
        if(maximum && bytes.byteLength>maximum)throw RangeError('Seed Pack exceeds receive limit');
        input(bytes);return new Pack(api.seed_open_stored_pack(definition.id,maximum));
      };
      return definition;
  }
  class Reads extends Handle {
    constructor() {super(18,api.seed_reads());}
    call(op,source='',address=0,value=0,claim) {const id=this.use();input(encoder.encode(source));return check(api.seed_ready(id,op,BigInt(addressText(address)),value,BigInt(claim?.use()??0)));}
    queue(source,address) {return ['unknown','queued','ready'][this.call(0,source,address)];}
    admit(source,address,bytes,claim) {return !!this.call(1,source,address,count32(bytes),claim);}
    load(source,address,pack,claim) {this.call(2,source,address,pack.use(),claim);}
    state(source,address) {return ['unknown','queued','ready'][this.call(3,source,address)];}
    coverage(source,address,flags) {return this.call(12,source,address,count32(flags));}
    published(source,address) {this.call(4,source,address);}
    submitted(source,address) {this.call(10,source,address);}
    discard(source,address,claim) {this.call(11,source,address,0,claim);}
    cancel(source,claim) {this.call(5,source,0,0,claim);}
    clear(source) {this.call(6,source);}
    used() {return this.call(7);}
    pack(source,address) {return new Pack(this.call(8,source,address));}
  }
  return {
    ports: {
      sample(bytes,sourceBytes,profile=null) {const length=input(bytes,encoder.encode(JSON.stringify(profile)));return capture(()=>api.seed_ports_sample(sourceBytes,length));},
      design(draft) {input(encoder.encode(JSON.stringify(draft)));return capture(()=>api.seed_ports_design());},
      review(mapping) {input(encoder.encode(JSON.stringify(mapping)));return capture(()=>api.seed_ports_review());},
    },
    reads:()=>new Reads(),
    stalks(query,options){if(!(query instanceof Query))throw TypeError('Expected a query from this Seed runtime');return query.stalks(options);},
    queryEvent(event){input(encoder.encode(JSON.stringify(event)));return capture(()=>api.seed_query_event());},
    pack: {
      size(prefix){input(prefix);return capture(()=>api.seed_pack_size());},
      expand(bytes,maximum=128*1024*1024){count32(maximum);input(bytes);return capture(()=>api.seed_pack_expand(maximum));},
    },
    summaryReport(spec) {
      input(encoder.encode(JSON.stringify({...spec,context:{referenceMs:Date.now(),...spec.context}})));const report=new Handle(14,api.seed_summary_report());
      report.read=(reader,selection)=>{const id=report.use(),r=reader.use();input(encoder.encode(JSON.stringify(selection,(_,v)=>typeof v==='bigint'?{$integer:v.toString()}:v)));check(api.seed_summary_report_read(id,r));return report;};
      report.result=()=>capture(()=>api.seed_summary_report_result(report.use()));return report;
    },
    summaryTable(spec) {
      input(encoder.encode(JSON.stringify({...spec,context:{referenceMs:Date.now(),...spec.context}})));const table=new Handle(17,api.seed_summary_table());
      table.read=(reader,selection)=>{const id=table.use(),r=reader.use();input(encoder.encode(JSON.stringify(selection)));check(api.seed_summary_table_read(id,r));return table;};
      table.remaining=()=>capture(()=>api.seed_summary_table_ranges(table.use()));
      table.events=batch=>check(api.seed_summary_table_batch(table.use(),batch.use()));
      table.result=()=>capture(()=>api.seed_summary_table_result(table.use()));return table;
    },
    summaryWindow(from,to) {
      if(!Number.isSafeInteger(from)||!Number.isSafeInteger(to)||from<0||to<0)throw RangeError('Invalid summary timestamps');
      return capture(()=>api.seed_summary_window(BigInt(from),BigInt(to)));
    },
    readControl(bytes) {input(bytes);return capture(()=>api.seed_control_read());},
    writeControl(value) {input(encoder.encode(JSON.stringify(value)));return capture(()=>api.seed_control_write());},
    trunk(bytes,options={}) {
      const writer=options.writer;
      if(writer){count32(writer.memoryBytes);count32(writer.batches);count32(writer.operations);}
      input(bytes);const trunk=new Handle(7,writer?api.seed_trunk_writer_open(writer.memoryBytes,writer.batches,writer.operations):api.seed_trunk_open());
      trunk.trie = () => {trunk.use();return trunk;};
      trunk.branches = () => {trunk.use();return new Branches(trunk);};
      trunk.query = query => {const id=trunk.use();input(encoder.encode(JSON.stringify(query)));return capture(()=>api.seed_trunk_query(id));};
      trunk.describe = id => capture(()=>api.seed_trunk_describe(trunk.use(),id));
      trunk.section = section => capture(()=>api.seed_trunk_section(trunk.use(),section));
      trunk.checkFile = (file,bytes) => {const id=trunk.use(),length=input(encoder.encode(file),bytes);check(api.seed_trunk_check_file(id,length));};
      trunk.prescribe = (id,bytes) => {const handle=trunk.use();input(bytes??new Uint8Array());return prepareDefinition(new Handle(0,api.seed_trunk_prescribe(handle,id)));};
      trunk.prescribeFile = (file,bytes) => {const handle=trunk.use(),length=input(encoder.encode(file),bytes);check(api.seed_trunk_prescribe_file(handle,length));};
      const commits={
        pending() {return check(api.seed_commit_pending(trunk.use()));},
        poll(calls=8,bytes=1024*1024) {const progress=capture(()=>api.seed_commit_poll(trunk.use(),count32(calls),count32(bytes)));if(progress.id!==undefined)progress.id=BigInt(progress.id);return progress;},
        published(id) {check(api.seed_commit_published(trunk.use(),BigInt(addressText(id))));},
        failed(id) {check(api.seed_commit_failed(trunk.use(),BigInt(addressText(id))));},
        recovered() {return capture(()=>api.seed_commit_recovered(trunk.use())).map(BigInt);},
      };
      trunk.commits=()=>{trunk.use();return commits;};
      return trunk;
    },
    storedLength(bytes) {input(bytes);return check(api.seed_stored_length());},
    beforeWrite(bytes) {input(bytes);check(api.seed_before_write());},
    prescribe(bytes, selection) {
      const length=input(bytes,selection===undefined?undefined:encoder.encode(JSON.stringify(selection)));
      const definition=new Handle(0,selection===undefined?api.seed_prescribe():api.seed_prescribe_version(length));
      return prepareDefinition(definition);
    },
    packs:store, beds:store,
    summaries(bytes,first,end) {
      if(!Number.isSafeInteger(first)||!Number.isSafeInteger(end)||first<0||end<first||end>0xffffffff)throw RangeError('Invalid summary Pack range');
      input(bytes);return new Store(api.seed_open_summaries(first,end));
    },
    get memoryBytes() {return api.memory.buffer.byteLength;},
  };
}
