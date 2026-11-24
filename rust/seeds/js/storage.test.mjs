import assert from 'node:assert/strict';
import {readFile} from 'node:fs/promises';
import {createSeeds} from './seeds.mjs';
const dir=process.argv[2];if(!dir)throw Error('Storage fixture directory required');
const read=name=>readFile(`${dir}/${name}`);
const runtime=await createSeeds(await readFile(new URL('./data-seeds.wasm',import.meta.url)));
const trunk=runtime.trunk(await read('trunk.trie'));
const [branch,id]=JSON.parse(await read('registration.json'));
const def=trunk.prescribe(id,await read('state.rx'));
const state=trunk.trie().branches().select([branch]).leaves();
const stored=await read('0.pac');
runtime.beforeWrite(stored);
const damaged=Buffer.from(stored),message=damaged.indexOf(Buffer.from('received'));
assert(message>=0);damaged[message]^=1;
assert.throws(()=>runtime.beforeWrite(damaged),/crc32c mismatch/);
// Opening published data performs structural checks, not a full CRC scan.
const readOnly=def.openStored(damaged);assert.equal(readOnly.info().length,2);readOnly.dispose();
const leaves=state.read(await read('0.leaf'));
const first=def.open(await read('0.pac'));
const bound=first.withIndex(await read('0.indx'),0,leaves);
first.dispose();
const store=runtime.beds().pack(bound);
const query=store.query().filter('region','=','west');
const batch=query.fetch(10);
const view=batch.views()[0];
assert.equal(view.region,'west');assert.equal(view.count,1n<<100n);assert.equal(view.time,0);
assert.deepEqual(batch.objects(),[{time:0,leaf_id:2,region:'west',count:1n<<100n,message:'received'}]);
const second=def.open(await read('1.pac'));
leaves.append(5,await read('1.leaves'));
const secondBound=second.withIndex(await read('1.indx'),3_600_000,leaves);
assert.equal(view.count,1n<<100n); // Existing readers retain their snapshot.
store.pack(secondBound);
const allQuery=store.query().orderBy('time').limit(10);const all=allQuery.objects(10);allQuery.dispose();
assert.equal(all.length,4);assert.equal(all[3].count,(1n<<100n)+3n);assert.equal(all[3].time,3_600_001);
const addresses=JSON.parse(await read('addresses.json'));
const fromBox=runtime.beds(await read('state.beds'),[def,def],addresses);
const fromBoxQuery=fromBox.query().filter('message','=','received');
assert.equal(fromBoxQuery.objects(10).length,4);
fromBoxQuery.dispose();fromBox.dispose();
const onePack=runtime.packs(await read('state.beds'),[def],[BigInt(addresses[1])]);
const oneQuery=onePack.query();assert.equal(oneQuery.objects(10).length,2);oneQuery.dispose();onePack.dispose();
leaves.dispose();def.dispose();second.dispose();secondBound.dispose();bound.dispose();
assert.equal(view.region,'west'); // Result lifetime retains its storage owners.
batch.dispose();query.dispose();store.dispose();
console.log('Rust state/index restoration: filtering, BigInt, views, objects, snapshots, and ownership passed');

trunk.dispose();
