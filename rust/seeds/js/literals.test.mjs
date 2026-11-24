import assert from 'node:assert/strict';
import {readFile} from 'node:fs/promises';
import {createSeeds} from './seeds.mjs';
const dir=process.argv[2];
const runtime=await createSeeds(await readFile(new URL('./data-seeds.wasm',import.meta.url)));
const definition=runtime.prescribe(await readFile(`${dir}/query.rx`));
const pack=definition.open(await readFile(`${dir}/query.pac`));
const beds=runtime.beds().pack(pack);
const values=[50000,'50000','50 ms',{$literal:{kind:'string',value:'50 ms'}},{$literal:{kind:'duration',value:'50ms'}}];
for(const value of values){
 const q=beds.query().filter('duration','>=',value).limit(3);const rows=q.objects();
 assert.deepEqual(rows.map(r=>r.duration),[50000,50001,50002]);q.dispose();
}
for(const [referenceMs,want] of [[1773072000000,1],[1773072000001,0]]){
 const q=beds.query().context({referenceMs,timezone:'UTC'}).filter('time','=','now').limit(1);
 assert.equal(q.objects().length,want);q.dispose();
}
const q=beds.query().filter('duration','>=','2 ft');assert.throws(()=>q.objects(),/cannot compare duration/);q.dispose();
beds.dispose();pack.dispose();definition.dispose();
console.log('WASM typed/string/duration literal parity, reference-clock freshness, and dimensional errors passed');
