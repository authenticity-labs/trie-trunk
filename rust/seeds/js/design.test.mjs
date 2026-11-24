import test from 'node:test';
import assert from 'node:assert/strict';
import {readFile} from 'node:fs/promises';
import {createSeeds} from './seeds.mjs';

const runtime=await createSeeds(await readFile(new URL('./data-seeds.wasm',import.meta.url)));
const sample=source=>runtime.ports.design({action:'sample',source});
const build=members=>Uint8Array.from(runtime.ports.design({action:'build',name:'account',members}));

test('DOC preserves nested objects and wide integers when reviewing structured samples',()=>{
 const source='{"owner":{"name":"A"},"amount":18446744073709551617,"checks":[true,false]}';
 const fromArray=sample('[\n'+source+',\n'+source+'\n]');
 const lines=sample(source+'\n'+source);
 assert.deepEqual(lines,fromArray);
 assert.equal(lines.records,2);
 const [account,owner]=lines.members;
 assert.equal(owner.path,'.owner');
 assert.equal(account.fields.find(f=>f.name==='amount').size,9);
 owner.fields[0].name='display_name';
 assert.ok(build(lines.members).length);
 assert.throws(()=>sample('[42]'),/JSON object/);
 assert.throws(()=>sample('{"x":1,"x":2}'),/duplicate/i);
});

test('scratch and sample drafts issue identical RX for the same structure',()=>{
 const members=[{path:'',fields:[{name:'label',kind:'tstr',size:4}]}];
 assert.deepEqual(build(members),build(sample('{"label":"test"}').members));
 assert.throws(()=>build([{path:'',fields:[...members[0].fields,...members[0].fields]}]),/unique/);
});

test('base bindings and historical RX inspection use the same Rust tools',async()=>{
 const design=draft=>runtime.ports.design(draft);
 const source=[...build(sample('{"label":"test"}').members)];
 for(const [base,size] of [['object',16],['event',10],['log',10]]){
  const bytes=design({action:'bind',base,authored:true,id:'identity',time:'timestamp',bytes:source});
  const {members,root}=design({action:'inspect',bytes,member:0});
  const fields=members[root].fields;
  assert.equal(fields.find(f=>f.name==='identity').type,'bytes('+size+')');
  assert.deepEqual(fields.find(f=>f.name==='identity').annotations,['@'+base]);
  assert.deepEqual(design({action:'bind',base,authored:false,id:'identity',time:'timestamp',bytes}),bytes);
  assert.equal(fields.some(f=>f.annotations.includes('@time')),base!=='object');
  assert.throws(()=>design({action:'bind',base:base==='object'?'event':'object',authored:false,id:'identity',time:'timestamp',bytes}),/conflicts/);
 }
 const bytes=[...await readFile(new URL('../../../testdata/doc/account-history.rx',import.meta.url))];
 for(const [member,name] of ['display_name','original_name'].entries()){
  const model=design({action:'inspect',bytes,member});
  assert.equal(model.members.length,1);
  assert.equal(model.members[model.root].fields[1].name,name);
 }
});
