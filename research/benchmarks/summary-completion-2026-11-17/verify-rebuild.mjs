import assert from 'node:assert/strict';
import {readFileSync as read} from 'node:fs';
import {createSeeds} from '/Users/Shared/code/applications/trie-trunk/rust/seeds/js/seeds.mjs';
const root=process.argv[2];
const runtime=await createSeeds(read('/Users/Shared/code/applications/trie-trunk/rust/seeds/js/data-seeds.wasm'));
const trunk=runtime.trunk(read(`${root}/trunk.trie`));
for(const file of ['indexed.rx','log-query.rx','summary.rx'])trunk.prescribeFile(file,read(`${root}/definitions/${file}`));
const results=[];
for(const report of JSON.parse(read(`${root}/summary-rebuild.json`))){
 const scope=trunk.trie().branches().select([report.branch]).summaries();
 const file=report.branch===1?'demo/lab/indexed/sample.summ':'demo/trie-trunk/queries/query.summ';
 const summary=scope.open(read(`${root}/${file}`),report.first);
 const info=summary.info();
 if(report.days){
  const rows=summary.rows(report.first,report.end);
  assert.equal(rows.reduce((sum,row)=>sum+row.count,0),report.events);
  results.push({branch:report.branch,events:report.events,periods:rows.length,fields:Object.keys(rows.find(r=>r.count).fields),info});
 }else{results.push({branch:report.branch,info});}
 summary.dispose();scope.dispose();
}
trunk.dispose();console.log(JSON.stringify(results,null,2));
