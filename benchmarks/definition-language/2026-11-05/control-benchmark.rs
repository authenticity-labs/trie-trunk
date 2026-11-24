use pil_codec::{authoring_language::{language as l,wire as w,cddl_import}, Definition as Layout, Field as LayoutField, Scalar};
use std::{alloc::{GlobalAlloc, Layout as AllocLayout,System},hint::black_box,sync::atomic::{AtomicBool,AtomicUsize,Ordering::Relaxed},time::Instant};
use serde_json::{json,Value};
use sha2::{Digest,Sha256};
struct Counting;
static ENABLED:AtomicBool=AtomicBool::new(false);
static ALLOCS:AtomicUsize=AtomicUsize::new(0);
static TOTAL:AtomicUsize=AtomicUsize::new(0);
static LIVE:AtomicUsize=AtomicUsize::new(0);
static PEAK:AtomicUsize=AtomicUsize::new(0);
fn add(n:usize){ALLOCS.fetch_add(1,Relaxed);TOTAL.fetch_add(n,Relaxed);let v=LIVE.fetch_add(n,Relaxed)+n;PEAK.fetch_max(v,Relaxed);}
unsafe impl GlobalAlloc for Counting {
 unsafe fn alloc(&self,l:AllocLayout)->*mut u8{let p=unsafe{System.alloc(l)};if !p.is_null()&&ENABLED.load(Relaxed){add(l.size())}p}
 unsafe fn alloc_zeroed(&self,l:AllocLayout)->*mut u8{let p=unsafe{System.alloc_zeroed(l)};if !p.is_null()&&ENABLED.load(Relaxed){add(l.size())}p}
 unsafe fn dealloc(&self,p:*mut u8,l:AllocLayout){if ENABLED.load(Relaxed){LIVE.fetch_sub(l.size(),Relaxed);}unsafe{System.dealloc(p,l)}}
 unsafe fn realloc(&self,p:*mut u8,l:AllocLayout,n:usize)->*mut u8{let q=unsafe{System.realloc(p,l,n)};if !q.is_null()&&ENABLED.load(Relaxed){LIVE.fetch_sub(l.size(),Relaxed);add(n)}q}
}
#[global_allocator]static A:Counting=Counting;
fn measure<T>(name:&str,mut f:impl FnMut()->T)->Value {
 for _ in 0..3 {black_box(f());}
 let start=Instant::now();for _ in 0..10{black_box(f());}
 let ns=(start.elapsed().as_nanos()/10).max(1);
 let iterations=(12_000_000/ns).clamp(5,100_000) as usize;
 let mut samples=Vec::new();
 for _ in 0..9 {let start=Instant::now();for _ in 0..iterations{black_box(f());}samples.push(start.elapsed().as_nanos() as f64/iterations as f64);}
 samples.sort_by(f64::total_cmp);
 ALLOCS.store(0,Relaxed);TOTAL.store(0,Relaxed);LIVE.store(0,Relaxed);PEAK.store(0,Relaxed);ENABLED.store(true,Relaxed);
 let result=black_box(f());
 let counts=(ALLOCS.load(Relaxed),TOTAL.load(Relaxed),LIVE.load(Relaxed),PEAK.load(Relaxed));
 drop(result);let outstanding=LIVE.load(Relaxed);ENABLED.store(false,Relaxed);assert_eq!(outstanding,0,"allocation balance {name}");
 json!({"operation":name,"median_ns":samples[4],"min_ns":samples[0],"max_ns":samples[8],"iterations_per_sample":iterations,"samples":samples,"allocations":counts.0,"allocated_bytes":counts.1,"retained_heap_bytes":counts.2,"peak_live_heap_bytes":counts.3})
}
fn definition(n:usize)->w::Definition{w::Definition{header:w::Header{rx_name:format!("fields{n}"),..Default::default()},fields:(0..n).map(|i|{let mut f=l::Field::scalar(&format!("field{i:03}"),l::UINT);f.size=4;f}).collect()}}
fn check(ds:&[w::Definition],bytes:&[u8]) {let decoded=w::read(bytes).unwrap();assert_eq!(w::write(&decoded).unwrap(),bytes);assert_eq!(w::hashes(ds).unwrap(),w::hashes(&decoded).unwrap());}
fn main(){
 let args:Vec<_>=std::env::args().collect();
 if args.get(1).map(String::as_str)==Some("cold") {let bytes=std::fs::read(&args[2]).unwrap();let start=Instant::now();let d=w::read(black_box(&bytes)).unwrap();println!("{}",json!({"cold_read_ns":start.elapsed().as_nanos(),"definitions":d.len()}));return;}
 let mut cases=Vec::new();for n in [6,32,128,512]{cases.push((format!("scalar_{n}"),vec![definition(n)]));}
 let base=definition(32);
 let mut d=base.clone();for f in &mut d.fields{f.size=0;}cases.push(("variable_width_32".into(),vec![d]));
 let mut d=base.clone();for f in &mut d.fields{f.annotations=vec!["@p95".into(),"@p98".into(),"#display(\"compact\")".into()];}assert_eq!(w::hashes(&[d.clone()]).unwrap(),w::hashes(&[base.clone()]).unwrap());cases.push(("annotated_32".into(),vec![d]));
 let mut d=base.clone();for f in d.fields.iter_mut().take(8){f.kind[0]|=l::HEADER;}cases.push(("header_prefix_32".into(),vec![d]));
 let mut d=base.clone();for f in &mut d.fields{f.kind=[l::ARRAY,l::UINT];f.size=0;f.element_size=Some(4);}cases.push(("arrays_uint4_32".into(),vec![d]));
 let mut d=base.clone();for f in &mut d.fields{f.kind=[l::TSTR,l::UINT];f.size=0;f.value_size=Some(4);}cases.push(("maps_tstr_uint4_32".into(),vec![d]));
 for count in [3,64]{let mut d=base.clone();for f in &mut d.fields{f.kind=[l::ENUM,l::TSTR];f.size=1;f.values=(0..count).map(|i|json!(format!("VALUE{i:03}"))).collect();}cases.push((format!("enums{count}_32"),vec![d]));}
 let child=definition(8);
 for n in [1,16]{let mut d=definition(n);d.header.rx_name="parent".into();for f in &mut d.fields{f.kind=[l::RECORD,0];f.size=0;f.values=child.fields.iter().map(|f|serde_json::to_value(f).unwrap()).collect();}cases.push((format!("inline_child8_x{n}"),vec![d.clone()]));for f in &mut d.fields{f.kind=[l::OBJECT,1];f.values.clear();}cases.push((format!("shared_child8_x{n}"),vec![d,child.clone()]));}
 let many:Vec<_>=(0..32).map(|i|{let mut d=definition(8);d.header.rx_name=format!("member{i}");d}).collect();cases.push(("bundle_32x8".into(),many));
 let mut rows=vec![];
 let prepared_rx=pil_codec::api::Doc::write(&w::member_layout().to_pil().unwrap()).unwrap();
 for (name,ds) in cases {
  let bytes=w::write(&ds).unwrap();check(&ds,&bytes);std::fs::write(format!("/tmp/pil-definition-perf/{name}.rx"),&bytes).unwrap();
  let text=serde_json::to_vec(&ds).unwrap();let mut ops=vec![];
  ops.push(measure("read_rx_verified",||w::read(black_box(&bytes)).unwrap()));
  rows.push(json!({"case":name,"rx_bytes":bytes.len(),"json_bytes":text.len(),"operations":ops}));
 }
 let mut old=vec![];
 for n in [6,32,128,512]{let d=Layout::new(format!("fields{n}"),0,(0..n).map(|i|LayoutField::new(format!("field{i:03}"),Scalar::U32)).collect()).unwrap();let b=d.to_pil().unwrap();old.push(json!({"fields":n,"bytes":b.len(),"operations":[measure("old_layout_write",||black_box(&d).to_pil().unwrap()),measure("old_layout_read",||Layout::from_pil(black_box(&b)).unwrap()),measure("old_prescription_register",||pil_codec::api::Doc::write(black_box(&b)).unwrap())]}));}
 let cddl=std::fs::read_to_string("/tmp/pil-definition-perf/log.cddl").unwrap();
 let ingest=measure("cddl_import_log6",||cddl_import::read(black_box(&cddl),None).unwrap());
 // Ordinal lookup in the already-read in-memory Definition. No record codec execution.
 let d=definition(512);let ords:Vec<_>=(0..4096).map(|i|(i*137)%512).collect();let access=measure("4096_decoded_descriptor_reads",||{let mut sum=0usize;for i in &ords{let f=black_box(&d.fields[*i]);sum+=f.size as usize+f.kind[0] as usize;}black_box(sum)});
 println!("{}",serde_json::to_string_pretty(&json!({"cases":rows,"established_runtime_baseline":old,"cddl":ingest,"descriptor_access":access,"meta_layout_register":measure("meta_layout_register",||pil_codec::api::Doc::write(&w::member_layout().to_pil().unwrap()).unwrap())})).unwrap());
}
