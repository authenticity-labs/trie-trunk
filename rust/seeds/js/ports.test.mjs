import assert from 'node:assert/strict';
import {readFile} from 'node:fs/promises';
import {createSeeds} from './seeds.mjs';

const {ports}=await createSeeds(await readFile(new URL('./data-seeds.wasm',import.meta.url)));
const bytes=new TextEncoder().encode('{"id":"0193a2b0-0000-7000-8000-000000000001","duration":25,"status":200}\n');
const sample=ports.sample(bytes,bytes.length);
assert.equal(sample.records,1);
assert.equal(sample.time[0].kind,'uuidv7');
assert.ok(sample.annotations.some(a=>a.value==='#summ(numeric)'));
const mapping={name:'http',time_field:'id',time_format:'uuidv7',fields:[
 {name:'id',kind:'tstr',size:36},
 {source:'duration',name:'elapsed',kind:'uint',size:4,annotations:['@unit(ms)','#summ(numeric)','#percentile(95)','@range(0,100)']},
 {name:'status',kind:'uint',size:2,values:['200','404'],annotations:['#summ(categorical)']},
]};
const review=ports.review(mapping);
assert.ok(review.fields.some(f=>f.name==='elapsed'&&f.annotations.includes('@unit(ms)')));
mapping.fields[1].annotations=['@range(100,0)'];
assert.throws(()=>ports.review(mapping),/Minimum exceeds maximum/);
assert.throws(()=>ports.sample(bytes,0),/sample size/);
console.log('Ports WASM: sampling, UUIDv7, catalogue, typed review, bounds, and sample limits passed');
