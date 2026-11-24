import assert from 'node:assert/strict';
import {readFile, mkdtemp, rm} from 'node:fs/promises';
import {openSync, closeSync, writeSync, fsyncSync, ftruncateSync, readFileSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {createSeeds} from './seeds.mjs';

const [wasm, fixtures]=process.argv.slice(2);
const [moduleBytes,trie,pac,origin]=await Promise.all([
  readFile(wasm), readFile(join(fixtures,'trunk.trie')),
  readFile(join(fixtures,'sample.pac')), readFile(join(fixtures,'sample.beds')),
]);
const directory=await mkdtemp(join(tmpdir(),'trunk-rust-writer-'));
const handles=[1,2].map(id=>({id,path:join(directory,`${id}.beds`)}));
for(const handle of handles)handle.fd=openSync(handle.path,'wx+');
let calls=[],failFlush=false;
try {
  const runtime=await createSeeds(moduleBytes,{files:id=>{
    const handle=handles.find(h=>h.id===id);assert.ok(handle);
    return {
      write(bytes,{at}) {const n=Math.min(bytes.length,7);calls.push(['write',id,at,n]);return writeSync(handle.fd,bytes,0,n,at);},
      truncate(length) {calls.push(['resize',id,length]);ftruncateSync(handle.fd,length);},
      flush() {calls.push(['flush',id]);if(failFlush)throw Error('injected OPFS-style quota/flush failure');fsyncSync(handle.fd);},
    };
  }});
  const trunk=runtime.trunk(trie,{writer:{memoryBytes:1024*1024,batches:1,operations:16}});
  const store=runtime.packs(), replica=store.reconstruct(origin.length,1);
  const first=replica.batch(trunk,1,8).load(0,pac).footer();
  const next=replica.batch(trunk,2,8).load(0,pac).footer();
  const firstId=first.submit();first.dispose();
  assert.throws(()=>next.submit(),/queue is full/);
  const commits=trunk.commits();
  let polls=0;
  while(true){const progress=commits.poll(2,11);polls++;if(progress.state==='ready'){assert.equal(progress.id,firstId);break;}assert.equal(progress.state,'writing');assert.ok(progress.calls<=2&&progress.bytes<=11);}
  assert.ok(polls>1);
  const completedCalls=calls.length;
  assert.equal(commits.poll().state,'ready');
  assert.equal(calls.length,completedCalls);
  assert.equal(readFileSync(handles[1].path).length,0);
  commits.published(firstId);
  const nextId=next.submit();next.dispose();
  while(commits.poll(8,1024).state!=='ready'){}
  commits.published(nextId);
  for(const handle of handles)assert.deepEqual(readFileSync(handle.path),origin);
  assert.equal(calls.filter(c=>c[0]==='flush').length,4);
  assert.ok(!calls.some(c=>c[0]==='resize'));
  assert.equal(commits.poll().state,'idle');

  // The receiver expands inside Rust. The writer accepts its handle, keeping
  // that allocation alive even after the host releases the Pack and receiver.
  const definition=trunk.prescribe(0,await readFile(join(fixtures,'sample.rx')));
  for(const encoded of [pac,await readFile(join(fixtures,'sample.zstd.pac'))]) {
    const receiver=definition.receiveForWrite(1024*1024);
    const received=receiver.load(encoded);assert.equal(receiver.finish(),1);receiver.dispose();
    assert.equal(received.length,1);
    received[0].bytes=()=>{throw Error('writer must not export Pack bytes');};
    const plan=replica.batch(trunk,1,4).load(0,received[0]);
    received[0].dispose();const id=plan.submit();plan.dispose();
    while(commits.poll(8,1024).state!=='ready'){}
    assert.deepEqual(readFileSync(handles[0].path),origin);
    commits.published(id);
  }
  definition.dispose();
  const compressedInput=replica.batch(trunk,1,4);
  const encoded=await readFile(join(fixtures,'sample.zstd.pac'));
  assert.throws(()=>compressedInput.load(0,encoded),/open compressed transport/);
  compressedInput.dispose();

  // A gap fill has no footer rewrite. Use the same ingested PAC at its origin
  // position; this checks the I/O boundary, not a second ingestion benchmark.
  calls=[];
  const gap=replica.batch(trunk,1,4).load(0,pac);const gapId=gap.submit();gap.dispose();
  while(commits.poll(8,1024).state!=='ready'){}
  assert.ok(calls.filter(c=>c[0]==='write').every(c=>c[2]+c[3]<=pac.length));
  assert.equal(calls.filter(c=>c[0]==='flush').length,1);
  commits.published(gapId);

  const failed=replica.batch(trunk,2,4).load(0,pac);const failedId=failed.submit();failed.dispose();
  failFlush=true;
  assert.throws(()=>{while(commits.poll(8,1024).state!=='ready'){}},/injected OPFS-style/);
  failFlush=false;
  assert.throws(()=>commits.poll(),/requires owner recovery/);
  assert.throws(()=>commits.published(failedId),/not awaiting publication/);
  // The fixture write was idempotent; published bytes are unchanged. In the
  // real owner this call follows restoring its metadata snapshot and file state.
  assert.deepEqual(readFileSync(handles[1].path),origin);
  assert.deepEqual(commits.recovered(),[failedId]);

  const huge=store.reconstruct((1n<<56n)+3n,1);
  const oversized=huge.batch(trunk,1,4).footer();oversized.submit();oversized.dispose();
  assert.throws(()=>commits.poll(),/host precision/);
  commits.recovered();huge.dispose();
  replica.dispose();store.dispose();trunk.dispose();
  console.log('Rust/WASM writer: sparse bytes, queue backpressure, publication barriers, failure recovery, and host precision checks passed');
} finally {
  for(const handle of handles)closeSync(handle.fd);
  await rm(directory,{recursive:true,force:true});
}
