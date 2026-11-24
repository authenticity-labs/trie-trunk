import assert from 'node:assert/strict';
import {readFile} from 'node:fs/promises';
import {createSeeds} from './seeds.mjs';
// Generated through native typed ingress in TestSeedLineagePublicationKeepsVersionsAndDefaultSeparate.
const directory=process.argv[2];
if(!directory)throw Error('Native lineage fixture directory required');
const runtime=await createSeeds(await readFile(new URL('./data-seeds.wasm',import.meta.url)));
const bytes=await readFile(`${directory}/log.http.rx`);
for(const [selection,file,value] of [
 [undefined,'current.pac',300],
 [{namespace:'log',name:'http',major:0,minor:1},'current.pac',300],
 [{namespace:'log',name:'http',major:0,minor:0},'previous.pac',200],
]) {
 const definition=runtime.prescribe(new DataView(bytes.buffer,bytes.byteOffset,bytes.byteLength),selection);
 const pack=definition.openStored(await readFile(`${directory}/${file}`));
 const store=runtime.beds();store.pack(pack);
 const cursor=store.query();const batch=cursor.fetch(10);
 try {assert.deepEqual(batch.objects(),[{status:value}]);assert.deepEqual(batch.views().map(v=>({...v})),[{status:value}]);}
 finally {batch.dispose();cursor.dispose();store.dispose();pack.dispose();definition.dispose();}
}
assert.throws(()=>runtime.prescribe(bytes,{namespace:'log',name:'http',major:0,minor:99}),/not found/);
assert.throws(()=>runtime.prescribe(bytes,{name:'http',major:0,minor:0}),/not found/);
console.log('Exact namespace/version selection -> native ingress Packs -> WASM views and objects passed');
