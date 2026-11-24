import assert from 'node:assert/strict';
import {readFile} from 'node:fs/promises';
import {createSeeds} from './seeds.mjs';
const [wasm,fixtures]=process.argv.slice(2);
const seed=await createSeeds(await readFile(wasm));
const definition=seed.prescribe(await readFile(`${fixtures}/sample.rx`));
const pack=definition.open(await readFile(`${fixtures}/sample.pac`));
const beds=seed.beds().pack(pack);
const reference=beds.query().filter('sequence','>=',95).select('sequence','reading','note','active').limit(310);
const expected=reference.objects(310);reference.dispose();
const query=beds.query().filter('sequence','>=',95).select('sequence','reading','note','active').limit(310);
const growth=seed.stalks(query,{rows:310});
assert.throws(()=>query.fetch(1),/Released/);
assert.throws(()=>growth.finish(),/incomplete/);
let polls=0;
while(!growth.poll(37).done){polls++;assert.ok(polls<20000);}
const stalk=growth.finish();assert.ok(polls>1);
// Independent owner handles can now go away. The columns still work.
beds.dispose();pack.dispose();definition.dispose();
assert.deepEqual(stalk.objects(0,310),expected);
assert.equal(stalk.info().rows,310);
assert.equal(BigInt(stalk.sum('sequence')),expected.reduce((n,r)=>n+BigInt(r.sequence),0n));
assert.equal(stalk.objects(310,1).length,0);
assert.throws(()=>stalk.objects(0,4097),/4096/);
assert.throws(()=>stalk.objects(311,1),/outside/);
assert.ok(stalk.info().bytes>0);stalk.dispose();assert.throws(()=>stalk.info(),/Released/);
// Failure/cancellation cleanup, and generation-safe handle reuse.
const d=seed.prescribe(await readFile(`${fixtures}/sample.rx`));
const p=d.open(await readFile(`${fixtures}/sample.pac`));const b=seed.beds().pack(p);
for(let i=0;i<50;i++){
 const g=b.query().select('sequence').stalks({rows:i%2?100:0});
 if(i%2){assert.equal(g.poll(3).done,false);g.cancel();}else{assert.throws(()=>g.poll(3),/budget/);g.dispose();}
 assert.throws(()=>g.finish(),/Released/);
}
b.dispose();p.dispose();d.dispose();
console.log('Stalks: filtered bounded growth, owned columns, scalar parity, paging, cancellation, failure and handle reuse passed');
