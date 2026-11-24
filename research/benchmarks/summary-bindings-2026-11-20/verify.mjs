import assert from 'node:assert/strict';
import {readFileSync as read} from 'node:fs';
import {createSeeds} from '../../../rust/seeds/js/seeds.mjs';
const root=process.argv[2], report=process.argv[3];
const runtime=await createSeeds(read(new URL('../../../rust/seeds/js/data-seeds.wasm',import.meta.url)));
const trunk=runtime.trunk(read(`${root}/trunk.trie`));
for(const file of ['indexed.rx','log-query.rx','summary.rx'])trunk.prescribeFile(file,read(`${root}/definitions/${file}`));
const results=[];
for(const item of JSON.parse(read(report))){
 const scope=trunk.trie().branches().select([item.branch]).summaries();
 const summary=scope.open(read(`${root}/${item.source}.v0.summ`),item.first);
 const rows=summary.rows(item.first,item.end);
 assert.equal(rows.reduce((n,r)=>n+Number(r.count),0),item.events);
 results.push({...item,periods:rows.length,fields:Object.keys(rows.find(r=>r.count)?.fields??{})});
 summary.dispose();scope.dispose();
}
trunk.dispose();console.log(JSON.stringify(results,null,2));
