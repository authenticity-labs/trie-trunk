//! Registered Box comparison using identical taxonomy and separately prescribed RX resources.
//! DOC authoring and registration are outside timing in both implementations.
//! All data is created through SummaryAccumulator::ingest, encoded and retained.
use data_seeds::{doc, Field, SeedDefinition, Predicate, Compare, summaries::*};
use ciborium::Value as Data;
use serde_json::{json, Value as Json};
use sha2::{Digest, Sha256};
use std::{alloc::{GlobalAlloc,Layout,System}, hint::black_box, sync::atomic::{AtomicBool,AtomicIsize,AtomicUsize,Ordering}, time::Instant};

struct Allocator;
static TRACK:AtomicBool=AtomicBool::new(false);
static LIVE:AtomicIsize=AtomicIsize::new(0);
static PEAK:AtomicIsize=AtomicIsize::new(0);
static CALLS:AtomicUsize=AtomicUsize::new(0);
static VOLUME:AtomicUsize=AtomicUsize::new(0);
fn add(bytes:usize){CALLS.fetch_add(1,Ordering::Relaxed);VOLUME.fetch_add(bytes,Ordering::Relaxed);let n=LIVE.fetch_add(bytes as isize,Ordering::Relaxed)+bytes as isize;PEAK.fetch_max(n,Ordering::Relaxed);}
unsafe impl GlobalAlloc for Allocator {
 unsafe fn alloc(&self,l:Layout)->*mut u8 {let p=unsafe{System.alloc(l)};if !p.is_null()&&TRACK.load(Ordering::Relaxed){add(l.size())}p}
 unsafe fn alloc_zeroed(&self,l:Layout)->*mut u8 {let p=unsafe{System.alloc_zeroed(l)};if !p.is_null()&&TRACK.load(Ordering::Relaxed){add(l.size())}p}
 unsafe fn dealloc(&self,p:*mut u8,l:Layout){if TRACK.load(Ordering::Relaxed){LIVE.fetch_sub(l.size() as isize,Ordering::Relaxed);}unsafe{System.dealloc(p,l)}}
 unsafe fn realloc(&self,p:*mut u8,l:Layout,n:usize)->*mut u8 {let q=unsafe{System.realloc(p,l,n)};if !q.is_null()&&TRACK.load(Ordering::Relaxed){LIVE.fetch_sub(l.size() as isize,Ordering::Relaxed);add(n)}q}
}
#[global_allocator]static ALLOC:Allocator=Allocator;
fn heap<T>(run:impl FnOnce()->T)->(T,Json){
 LIVE.store(0,Ordering::Relaxed);PEAK.store(0,Ordering::Relaxed);CALLS.store(0,Ordering::Relaxed);VOLUME.store(0,Ordering::Relaxed);TRACK.store(true,Ordering::Relaxed);
 let output=run();TRACK.store(false,Ordering::Relaxed);
 (output,json!({"retained_delta_bytes":LIVE.load(Ordering::Relaxed),"peak_extra_bytes":PEAK.load(Ordering::Relaxed),"allocation_calls":CALLS.load(Ordering::Relaxed),"allocation_volume_bytes":VOLUME.load(Ordering::Relaxed)}))
}
fn us(start:Instant)->f64{start.elapsed().as_secs_f64()*1e6}
fn digest(bytes:&[u8])->String{format!("{:x}",Sha256::digest(bytes))}
#[derive(Clone,Copy)]struct Case {name:&'static str,fields:usize,days:usize,sparse:bool}
fn source(c:Case)->(std::sync::Arc<SeedDefinition>,data_seeds::registration::Trunk,u16){
 let mut d=doc::new("summary-comparison");
 for i in 0..c.fields {let field=Field::new(if i==0{"duration".into()}else{format!("metric_{i}")});let field=match i%4{0=>field.r#type("uint").size(4),1=>field.r#type("uint").size(3),2=>field.r#type("int").size(6),_=>field.r#type("float").size(8)};d=d.field(field.annotation("#summ(numeric)"));}
 d=d.field(Field::new("status").closed_enum("uint",[200,201,204,301,400,401,404,500].map(Into::into)).annotation("#summ(categorical)"))
  .field(Field::new("cached").r#type("bool").annotation("#summ(boolean)"));
 let document=d.sign().unwrap();
 let (taxonomy,resource)=trie_trunk_taxonomy::Trunk::new("Benchmark").unwrap().registration(
  trie_trunk_taxonomy::registration::Registration{cache:"test",service:"app",resource:"events",resource_type:"http",level:"",file:"events.rx"},&document).unwrap();
 let library=summary_template().unwrap();
 let taxonomy=taxonomy.register_summaries(resource.branch,"summary.rx",&library).unwrap();
 let mut trunk=data_seeds::registration::Trunk::open(taxonomy.bytes()).unwrap();
 trunk.prescribe_file("events.rx",document.bytes().unwrap()).unwrap();
 trunk.prescribe_file("summary.rx",library.bytes().unwrap()).unwrap();
 (trunk.definition(resource.definition).unwrap(),trunk,resource.branch)
}
struct Fixture {bytes:Vec<u8>,events:Vec<Vec<u8>>,timing:Json,event_count:usize}
fn fixture(c:Case,source:&std::sync::Arc<SeedDefinition>)->Fixture{
 let total=Instant::now();let t=Instant::now();let plan=source.summaries(288).unwrap();let prepare=us(t);
 let t=Instant::now();let mut storage=SummaryBox::new(plan.layout().clone()).unwrap();let mut seal=us(t);
 let mut ingress=0.;let mut events=Vec::new();let mut event_count=0;
 for day in 0..c.days {
  let t=Instant::now();let mut accumulator=plan.accumulator(day as u64*SUMMARY_DAY_MS).unwrap();
  let count=if c.sparse {12*8}else{288*8};let mut event_pack=source.fill(count).unwrap();
  for period in 0..288 {if c.sparse&&period%24!=0 {continue} for event in 0..8 {
   let sequence=day*288*8+period*8+event;let time=day as u64*SUMMARY_DAY_MS+period as u64*SUMMARY_PERIOD_MS+event as u64;
   let pill=accumulator.ingest(time,sequence,|seq,pill|{
    for field in 0..c.fields {
     if field>0&&(seq+field)%19==0 {continue}
     if field>0&&(seq+field)%23==0 {pill.set_null(field)?;continue}
     let n=((seq*31+field*17)%9001) as u32;
     match field%4 {0|1=>{pill.set_uint(field,n)?;},2=>{pill.set_int(field,n as i64-4500)?;},_=>{pill.set_float(field,n as f64+0.25)?;}}
    }
    pill.set_enum(c.fields,[200u32,201,204,301,400,401,404,500][seq%8].into())?;
    pill.set_bool(c.fields+1,seq%3==0)?;Ok(())
   }).unwrap();event_pack.load(pill).unwrap();event_count+=1;
  }}
  event_pack.seal().unwrap();events.push(event_pack.into_bytes().unwrap());ingress+=us(t);
  let t=Instant::now();accumulator.complete(accumulator.end()).unwrap();storage.append(accumulator.seal().unwrap()).unwrap();seal+=us(t);
 }
 let t=Instant::now();storage.commit().unwrap();let bytes=storage.bytes().unwrap().to_vec();let publish=us(t);
 Fixture{bytes,events,event_count,timing:json!({"prepare_us":prepare,"ingress_and_accumulate_us":ingress,"summary_seal_and_append_us":seal,"box_commit_and_owned_bytes_us":publish,"build_total_us":us(total)})}
}
fn zero(reader:&mut SummaryReader,w:&SummaryWindow)->Json{
 let p=reader.preview(w,&Predicate::compare("duration",Compare::Gt,10000u32)).unwrap();
 assert_eq!(p.count,Some(CountBounds::exact(0)));assert!(p.eligible.is_empty());assert!(p.coverage_complete);
 json!({"zero":true,"periods":p.periods.len()})
}
fn category(reader:&mut SummaryReader,w:&SummaryWindow,count:usize)->Json{
 let p=reader.preview(w,&Predicate::compare("status",Compare::Eq,500u32)).unwrap();
 assert_eq!(p.count,Some(CountBounds::exact((count/8) as u64)));assert!(p.coverage_complete);
 json!({"matches":count/8,"eligible_periods":p.periods.iter().filter(|p|p.count.upper>0).count()})
}
fn rows(reader:&mut SummaryReader,w:&SummaryWindow,fields:Option<&[String]>)->Vec<Data>{
 let mut result=Vec::new();reader.projected_rows(w,fields,|time,count,object|{
  result.push(Data::Array(vec![time.into(),count.into(),Data::Map(object.into_iter().map(|(k,v)|(k.into(),v)).collect())]));Ok(())
 }).unwrap();result
}
fn timed<T>(f:impl FnOnce()->T)->(T,f64){let t=Instant::now();let v=f();(v,us(t))}
fn run(c:Case)->Json {
 let (source,trunk,branch)=source(c);let f=fixture(c,&source);let w=SummaryWindow::new(0..c.days as u64*SUMMARY_DAY_MS).unwrap();
 let mut times=f.timing.as_object().unwrap().clone();
 let (mut reader,open)=timed(||SummaryReader::registered_box(&trunk,branch,f.bytes.clone(),0).unwrap());times.insert("open_us".into(),json!(open));
 let (_,first)=timed(||zero(&mut reader,&w));times.insert("first_zero_preview_us".into(),json!(first));
 times.insert("cold_zero_total_us".into(),json!(open+first));
 let (_,warm)=timed(||{for _ in 0..10 {black_box(zero(&mut reader,&w));}});times.insert("warm_zero_preview_us".into(),json!(warm/10.));
 let (category_result,category_time)=timed(||category(&mut reader,&w,f.event_count));times.insert("category_preview_us".into(),json!(category_time));
 let (selected,selected_time)=timed(||rows(&mut reader,&w,Some(&["duration".into()])));times.insert("one_field_objects_us".into(),json!(selected_time));black_box(selected);
 let (all,all_time)=timed(||rows(&mut reader,&w,None));times.insert("all_field_objects_us".into(),json!(all_time));
 let mut canonical=Vec::new();ciborium::into_writer(&all,&mut canonical).unwrap();let decoded_digest=digest(&canonical);drop(all);
 let (dashboard,dashboard_time)=timed(||{
  let mut report=SummaryReport::new(0,c.days as u64*SUMMARY_DAY_MS,3_600_000,"duration").unwrap();
  report.consume(&mut reader,"benchmark",0,c.days as u64*SUMMARY_DAY_MS,&Predicate::All).unwrap();
  let mut last=Json::Null;
  for _ in 0..10 {let result=report.finish().unwrap();assert_eq!(result["dashboard"]["totals"]["events"],f.event_count);last=black_box(result);}
  last
 });times.insert("dashboard_collect_plus_ten_reductions_us".into(),json!(dashboard_time));
 let mut event_hash=Sha256::new();for pack in &f.events {event_hash.update(pack)}
 let event_bytes:usize=f.events.iter().map(Vec::len).sum();
 let (plan,plan_heap)=heap(||source.summaries(288).unwrap());let (_,accumulator_heap)=heap(||plan.accumulator(0).unwrap());drop(plan);
 let (_,cold_heap)=heap(||{let mut reader=SummaryReader::registered_box(&trunk,branch,f.bytes.clone(),0).unwrap();zero(&mut reader,&w);reader});
 let (_,projection_heap)=heap(||rows(&mut reader,&w,None));
 let summary_schema=source.summaries(288).unwrap().document().bytes().unwrap().len();
 json!({"case":c.name,"numeric_fields":c.fields,"total_fields":c.fields+2,"days":c.days,"summary_rows":c.days*288,"events":f.event_count,"event_bytes":event_bytes,"event_sha256":format!("{:x}",event_hash.finalize()),"summary_box_bytes":f.bytes.len(),"summary_definition_bytes":summary_schema,"decoded_sha256":decoded_digest,"dashboard_sha256":digest(&serde_json::to_vec(&dashboard).unwrap()),"category":category_result,"timing":times,"heap":{"plan":plan_heap,"empty_day_accumulator":accumulator_heap,"cold_zero_query":cold_heap,"all_field_objects":projection_heap}})
}
fn main(){
 let case=std::env::args().nth(1).unwrap_or_else(||"typical".into());
 let c=match case.as_str(){"narrow"=>Case{name:"narrow",fields:1,days:1,sparse:false},"typical"=>Case{name:"typical",fields:8,days:7,sparse:false},"wide"=>Case{name:"wide",fields:32,days:7,sparse:false},"quiet"=>Case{name:"quiet",fields:8,days:7,sparse:true},_=>panic!("unknown case")};
 if std::env::var_os("SUMMARY_HEAP_INIT").is_some() {
  let (source,_trunk,_branch)=source(c);
  let (_,init)=heap(|| {let plan=source.summaries(288).unwrap();black_box(&plan);drop(plan);});
  println!("{}",json!({"case":c.name,"shared_schema_initialization":init}));
 } else { println!("{}",run(c)); }
}
