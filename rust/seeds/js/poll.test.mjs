import assert from 'node:assert/strict';
import {readFile} from 'node:fs/promises';
import {createSeeds} from './seeds.mjs';
const [wasm,fixtures]=process.argv.slice(2);
const runtime=await createSeeds(await readFile(wasm));
const definition=runtime.prescribe(await readFile(`${fixtures}/sample.rx`));
const pack=definition.open(await readFile(`${fixtures}/sample.pac`));
const store=runtime.beds().pack(pack);
const sync=store.query().select('sequence').orderBy('sequence','desc').offset(3).limit(17);
const expected=sync.objects(100);sync.dispose();
const sliced=store.query().select('sequence').orderBy('sequence','desc').offset(3).limit(17);
const got=[];let pending=0;
for(let i=0;i<20000;i++) {
  const slice=sliced.poll(4,11);assert.ok(slice.work<=11);
  if(slice.batch){try{got.push(...slice.batch.objects());}finally{slice.batch.dispose();}}
  else if(!slice.done)pending++;
  if(slice.done)break;
  assert.ok(i<19999,'query did not complete');
}
assert.deepEqual(got,expected);assert.ok(pending>1);
assert.deepEqual(sliced.stats(),{scanned:pack.info().length,matched:pack.info().length});
sliced.dispose();
const slow=store.query().filter('sequence','>',1000000);
assert.deepEqual(slow.poll(1,1),{batch:null,done:false,work:1});
slow.cancel();assert.throws(()=>slow.poll(1,1),/cancelled/);slow.dispose();
// Reusing cursor/result handle slots must not invalidate separately held views.
const owner=store.query().select('sequence').limit(1);
const held=owner.poll(1,1).batch;const view=held.views()[0];owner.dispose();
for(let i=0;i<5000;i++) {
  const q=store.query().limit(1);const {batch,done}=q.poll(1,1);
  assert.equal(done,true);batch.dispose();q.dispose();
}
assert.equal(view.sequence,0);held.dispose();assert.throws(()=>view.sequence,/Released/);
const rejected=store.query().orderBy('sequence').limit(10000000);
assert.throws(()=>rejected.poll(1,1),/budget/);rejected.dispose();
store.dispose();pack.dispose();definition.dispose();
console.log('WASM sliced sorting, pending versus done, cancellation, budget rejection, and reusable handle lifetimes passed');
