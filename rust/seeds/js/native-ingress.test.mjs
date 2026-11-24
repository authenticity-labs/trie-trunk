import assert from 'node:assert/strict';
import {readFile} from 'node:fs/promises';
import {createSeeds} from './seeds.mjs';

// Fixture generation is TestSeedNativeIngressPackControls in the server adapter:
// Rust DOC -> typed Seed ingress -> sealed plain/Zstd Packs. No host wire encoder.
const directory=process.argv[2];
if(!directory)throw Error('Native ingress fixture directory required');
const runtime=await createSeeds(await readFile(new URL('./data-seeds.wasm',import.meta.url)));
const definition=runtime.prescribe(await readFile(`${directory}/definition.rx`));
const wire=await readFile(`${directory}/seeds.pacs`);
for(const chunkSize of [1,2,7,31,wire.length]) {
  const receiver=definition.receiveStored(64*1024*1024),store=runtime.beds();
  try {
    let count=0;
    for(let at=0;at<wire.length;at+=chunkSize) {
      for(const pack of receiver.load(wire.subarray(at,at+chunkSize))) {
        const info=pack.info();
        assert.equal(info.hour,1762732800000);
        assert.equal(info.sealed,true);
        store.pack(pack);pack.dispose();count++;
      }
    }
    assert.equal(receiver.finish(),2);assert.equal(count,2);
    const query=store.query().filter('duration','>',10000).orderBy('time').limit(20);
    try {
      const batch=query.fetch(20);
      try {
        const objects=batch.objects(),views=batch.views();
        assert.equal(objects.length,4);
        assert.deepEqual(objects.map(o=>o.duration),[10001,10001,10002,10002]);
        assert.deepEqual(views.map(o=>({...o})),objects);
        assert(objects.every(o=>o.message==='received'&&o.id.length===16));
        for(const o of objects){
          assert.equal(o.id[6]>>>4,7);assert.equal(o.id[8]>>>6,2);
          assert.equal(Number(Buffer.from(o.id).readUIntBE(0,6)),o.time);
        }
      } finally {batch.dispose();}
    } finally {query.dispose();}
  } finally {receiver.dispose();store.dispose();}
}
// Query opens do not scan CRCs. Publication checks are exercised separately
// by storage.test.mjs before writing a complete Pack.
const damaged=Buffer.from(wire),message=damaged.indexOf(Buffer.from('received'));
assert(message>=0);damaged[message]^=1;
const reopened=definition.receiveStored(64*1024*1024);
try {for(const pack of reopened.load(damaged))pack.dispose();assert.equal(reopened.finish(),2);} finally {reopened.dispose();}
const limited=definition.receiveStored(65_536);
const largeWire=await readFile(`${directory}/limited.pac`);
assert.throws(()=>definition.openStored(largeWire,65_536),/receive limit/);
try {assert(largeWire.length<65_536);assert.throws(()=>limited.load(largeWire),/receive limit/);} finally {limited.dispose();}
definition.dispose();
console.log('Native typed ingress -> plain/Zstd adjacent PACs -> fragmented WASM receiver -> Rust filter -> views/objects passed');
