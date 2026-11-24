//! Resident daily-Pack reader benchmark. Typed events pass through ingestion;
//! data preparation is outside these read-only timings in both implementations.
use data_seeds::{doc,Field,SeedDefinition,Predicate,Compare,summaries::*};
use serde_json::json;
use sha2::{Digest,Sha256};
use std::{hint::black_box,time::Instant};
fn us(start:Instant)->f64 { start.elapsed().as_secs_f64()*1e6 }
#[derive(Clone,Copy)]struct Case {name:&'static str,fields:usize,days:usize,sparse:bool}
fn source(c:Case)->(std::sync::Arc<SeedDefinition>,data_seeds::registration::Trunk,u16){
 let mut d=doc::new("summary-comparison");
 for i in 0..c.fields {let field=Field::new(if i==0{"duration".into()}else{format!("metric_{i}")});let field=match i%4{0=>field.r#type("uint").size(4),1=>field.r#type("uint").size(3),2=>field.r#type("int").size(6),_=>field.r#type("float").size(8)};d=d.field(field.annotation("#summ(numeric)"));}
 d=d.field(Field::new("status").enumeration("uint",[200,201,204,301,400,401,404,500].map(Into::into)).annotation("#summ(categorical)"))
  .field(Field::new("cached").r#type("bool").annotation("#summ(boolean)"));
 let document=d.sign().unwrap();
 let (taxonomy,resource)=trie_trunk_taxonomy::Trunk::new("Benchmark").unwrap().registration(
  trie_trunk_taxonomy::registration::Registration{cache:"test",service:"app",resource:"events",resource_type:"http",level:"",file:"events.rx"},&document).unwrap();
 let library=summary_template().unwrap();
 let taxonomy=taxonomy.register_summaries(resource.branch,"summary.rx",&library).unwrap();
 let taxonomy=taxonomy.register_summary_context(resource.branch,trie_trunk_taxonomy::registration::SummaryContext {
  first:0,end:c.days as u64*SUMMARY_DAY_MS,sources:vec![[resource.definition,0]],sealed:true }).unwrap();
 let mut trunk=data_seeds::registration::Trunk::open(taxonomy.bytes()).unwrap();
 trunk.prescribe_file("events.rx",document.bytes().unwrap()).unwrap();
 trunk.prescribe_file("summary.rx",library.bytes().unwrap()).unwrap();
 (trunk.definition(resource.definition).unwrap(),trunk,resource.branch)
}
fn main(){
 let name=std::env::args().nth(1).unwrap_or_else(||"typical".into());
 let c=match name.as_str(){"narrow"=>Case{name:"narrow",fields:1,days:1,sparse:false},"typical"=>Case{name:"typical",fields:8,days:7,sparse:false},"wide"=>Case{name:"wide",fields:32,days:7,sparse:false},"quiet"=>Case{name:"quiet",fields:8,days:7,sparse:true},_=>panic!("unknown case")};
 let (source,trunk,branch)=source(c);let plan=source.summaries(288).unwrap();
 let mut packets=Vec::new();let mut events=Vec::new();let mut count=0;
 for day in 0..c.days {
  let first=day as u64*SUMMARY_DAY_MS;let mut accumulator=plan.accumulator(first).unwrap();let mut pack=source.fill(288*8).unwrap();
  for period in 0..288 {if c.sparse&&period%24!=0{continue} for n in 0..8 {
   let sequence=day*288*8+period*8+n;
   let pill=accumulator.ingest(first+period as u64*SUMMARY_PERIOD_MS+n as u64,sequence,|seq,pill|{
    for f in 0..c.fields {let value=((seq*31+f*17)%9001) as u32;
     if f>0&&(seq+f)%19==0 {continue} if f>0&&(seq+f)%23==0{pill.set_null(f)?;continue}
     match f%4{0|1=>{pill.set_uint(f,value)?;},2=>{pill.set_int(f,value as i64-4500)?;},_=>{pill.set_float(f,value as f64+0.25)?;}}
    }
    pill.set_enum(c.fields,[200u32,201,204,301,400,401,404,500][seq%8].into())?;
    pill.set_bool(c.fields+1,seq%3==0)?;Ok(())
   }).unwrap();pack.load(pill).unwrap();count+=1;
  }}
  pack.seal().unwrap();events.push(pack.into_bytes().unwrap());
  accumulator.complete(accumulator.end()).unwrap();
  packets.push(accumulator.seal_registered(&trunk.trie(),branch).unwrap().bytes().to_vec());
 }
 let windows=(0..c.days).map(|i|SummaryWindow::new(i as u64*SUMMARY_DAY_MS..(i+1) as u64*SUMMARY_DAY_MS).unwrap()).collect::<Vec<_>>();let predicate=Predicate::compare("duration",Compare::Gt,10000u32);
 let t=Instant::now();let mut readers=packets.iter().enumerate().map(|(i,p)|SummaryReader::registered(&trunk,branch,p.clone(),i as u64*SUMMARY_DAY_MS).unwrap()).collect::<Vec<_>>();let open=us(t);
 let t=Instant::now();for (reader,window) in readers.iter_mut().zip(&windows){let r=reader.preview(window,&predicate).unwrap();assert_eq!(r.count,Some(CountBounds::exact(0)));assert!(r.eligible.is_empty());black_box(r);}let query=us(t);
 let t=Instant::now();for _ in 0..100 {for (reader,window) in readers.iter_mut().zip(&windows){black_box(reader.preview(window,&predicate).unwrap());}}let warm=us(t)/100.;
 let mut event_hash=Sha256::new();for p in &events{event_hash.update(p);}let mut packet_hash=Sha256::new();for p in &packets{packet_hash.update(p);}
 println!("{}",json!({"case":c.name,"events":count,"days":c.days,"event_sha256":format!("{:x}",event_hash.finalize()),"pac_sha256":format!("{:x}",packet_hash.finalize()),"bytes":packets.iter().map(Vec::len).sum::<usize>(),"open_us":open,"cold_query_us":query,"cold_total_us":open+query,"warm_query_us":warm}));
}
