// Footprint measurement only. Run: node measure-million.mjs /path/to/pill-packer [count]
import {readFileSync, writeFileSync} from 'node:fs';
import {resolve} from 'node:path';
import {pathToFileURL} from 'node:url';
import {createHash} from 'node:crypto';
import {zstdCompressSync, zstdDecompressSync, constants} from 'node:zlib';
import assert from 'node:assert/strict';
const root=resolve(process.argv[2]);
const count=Number(process.argv[3] ?? 1_000_000), batchSize=1000;
assert(Number.isSafeInteger(count) && count>0);
const load=path=>import(pathToFileURL(resolve(root,'npm',path)));
const {createCodec}=await load('runtime.js');
const {define,pack,Prescription,absent,nullValue}=await load('portable/pack.js');
const {expandPAC}=await load('portable/pac.js');
const wasm=readFileSync(resolve(root,'npm/codec.wasm'));
const codec=await createCodec(wasm);
const definition={pil:0,definition:'example/orders',version:0,fields:[
 {name:'id',type:'U32'}, {name:'created',type:'U64'},
 {name:'customer',type:'U32'}, {name:'product',type:'U32'},
 {name:'quantity',type:'U16'}, {name:'price',type:'U32'},
 {name:'status',type:'STRING',values:['pending','paid','shipped','returned']},
 {name:'currency',type:'STRING',values:['USD','EUR','GBP']},
 {name:'region',type:'STRING',values:['north','south','east','west']},
 {name:'reference',type:'STRING'}, {name:'message',type:'STRING'},
 {name:'note',type:'STRING'}
]};
const defBytes=codec.definitionFromJSON(JSON.stringify(definition));
const def=await define(defBytes,codec);
const rx=new Prescription(def),text=new TextEncoder();
// The public fill API takes primitive field buffers; the codec owns the Pill layout.
const encode=obj=>rx.fill(definition.fields.map(field=>{
 if(!Object.hasOwn(obj,field.name))return absent;
 const value=obj[field.name];if(value===null)return nullValue;
 if(field.values)return Uint8Array.of(field.values.indexOf(value));
 if(field.type==='STRING')return text.encode(value);
 const bytes=new Uint8Array(Number(field.type.slice(1))/8),view=new DataView(bytes.buffer);
 if(bytes.length===2)view.setUint16(0,value,true);
 else if(bytes.length===4)view.setUint32(0,value,true);
 else view.setBigUint64(0,BigInt(value),true);
 return bytes;
})).bytes;
let rng=0x10203040;
const random=()=>{rng^=rng<<13;rng^=rng>>>17;rng^=rng<<5;return rng>>>0;};
const sourceHash=createHash('sha256');
const totals={json:0,json_zstd1:0,pills:defBytes.length,pil:defBytes.length,pil_zstd1:defBytes.length};
let batches=0;
try {
 for(let start=0;start<count;start+=batchSize){
  const frames=[],lines=[],objects=[];
  for(let i=start;i<Math.min(start+batchSize,count);i++){
   const obj={id:i+1,created:1793491200000+i*17,customer:random()%100000,
    product:random()%10000,quantity:1+random()%20,price:100+random()%100000,
    status:definition.fields[6].values[random()%4],currency:definition.fields[7].values[random()%3],
    region:definition.fields[8].values[random()%4],reference:random().toString(16).padStart(8,'0'),
    message:['Order received','Payment accepted','Parcel dispatched','Return requested'][random()%4]};
   if(i%5===0)obj.note=null;else if(i%3===0)obj.note='Leave at '+['reception','front door','loading bay'][random()%3];
   const line=JSON.stringify(obj);
   frames.push(encode(obj)); lines.push(line); objects.push(obj);
  }
  const json=Buffer.from(lines.join('\n')+'\n');sourceHash.update(json);
  const jsonZstd=zstdCompressSync(json,{params:{[constants.ZSTD_c_compressionLevel]:1}});
  assert.deepEqual(zstdDecompressSync(jsonZstd),json);
  const packed=pack(def,frames,{packing:{commonValues:true}});
  const sealed=codec.seal(packed.bytes,514,1,false);
  const expanded=await expandPAC(sealed,0,packed.bytes.length+65536,(b,n,f)=>codec.decompress(b,n,f));
  assert.deepEqual(expanded.bytes,packed.bytes);
  for(let j=0;j<frames.length;j++)assert.deepEqual(rx.fill(packed.get(j)).crush(),objects[j]);
  totals.json+=json.length;totals.json_zstd1+=jsonZstd.length;
  totals.pills+=frames.reduce((sum,frame)=>sum+frame.length,0);
  totals.pil+=packed.bytes.length;totals.pil_zstd1+=sealed.length;
  batches++;
  if(batches%100===0)process.stderr.write(`${Math.min(start+batchSize,count)} objects verified\n`);
 }
} finally {codec.release(def.id);}
const result={date:new Date().toISOString().slice(0,10),count,batch_size:batchSize,batches,
 dataset:'Deterministic synthetic orders: twelve fields, varied numbers and strings, optional and null notes. Not a timing benchmark.',
 definition,definition_bytes:defBytes.length,totals,source_sha256:sourceHash.digest('hex'),
 verification:'Every Pill decoded and compared to its source object; every JSON and PAC compression batch decompressed and compared byte-for-byte.',
 accounting:'PIL includes PAC framing and one shared Definition. JSON is compact JSONL. Both formats use identical groups. No external indexes or payload data omitted.',
 compression:{level:1,json:`Node ${process.version}, Zstd ${process.versions.zstd}`,pil:'Existing Pill Packer Rust/WASM PAC sealer; Zstd level 1'},
 wasm_sha256:createHash('sha256').update(wasm).digest('hex'),
 script_sha256:createHash('sha256').update(readFileSync(new URL(import.meta.url))).digest('hex')};
writeFileSync(new URL('./million-objects-zstd.json',import.meta.url),JSON.stringify(result,null,2)+'\n');
console.log(JSON.stringify(result.totals));
