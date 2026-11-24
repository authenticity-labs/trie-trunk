//! End-to-end local import benchmark. The output directory must not exist.
use data_seeds::{ports::import::{Column,Mapping},Predicate,Query};
use std::{fs::File,io::{Read,Write,BufWriter},time::Instant};
use trunk_node::{Destination,Node,Result};
fn main()->Result<()> {
    let root=std::path::PathBuf::from(std::env::args().nth(1).ok_or("Output directory required")?);
    let rows:usize=std::env::args().nth(2).map(|s|s.parse()).transpose()?.unwrap_or(240_000);
    let day=86_400_000u64;
    let from=std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH)?.as_millis() as u64/day*day-2*day;
    let started=Instant::now();let node=Node::install(&root,"Standalone Benchmark")?;
    let install_ms=started.elapsed().as_secs_f64()*1000.;
    let source=root.join("sample.jsonl");let started=Instant::now();
    let mut file=BufWriter::new(File::create(&source)?);
    for n in 0..rows {writeln!(file,"{{\"timestamp\":{},\"duration\":{},\"success\":{},\"message\":\"sample event\"}}",from+n as u64*(day-1)/rows as u64,n%20_000,n%10!=0)?;}
    file.flush()?;drop(file);let generation_ms=started.elapsed().as_secs_f64()*1000.;
    let mut file=File::open(&source)?;let bytes=file.metadata()?.len();
    let mapping=Mapping{format:Default::default(),log:false,name:"sample".into(),time_field:"timestamp".into(),time_format:"unix_ms".into(),fields:vec![
        Column{name:"timestamp".into(),kind:"uint".into(),size:6,summarize:false, ..Default::default()},
        Column{name:"duration".into(),kind:"uint".into(),size:4,summarize:true, ..Default::default()},
        Column{name:"success".into(),kind:"bool".into(),size:0,summarize:true, ..Default::default()},
        Column{name:"message".into(),kind:"tstr".into(),size:12,summarize:false, ..Default::default()},
    ]};
    let started=Instant::now();
    let token=node.upload(Destination{cache:"local".into(),service:"bench".into(),resource:"events".into(),resource_type:"sample".into(),file:"sample.rx".into()},mapping,bytes)?;
    let mut offset=0;let mut chunk=vec![0;262144];
    loop{let n=file.read(&mut chunk)?;if n==0{break;}node.append(&token,offset,&chunk[..n])?;offset+=n as u64;}
    let upload_ms=started.elapsed().as_secs_f64()*1000.;
    let started=Instant::now();let result=node.import(&token)?;let import_ms=started.elapsed().as_secs_f64()*1000.;
    assert_eq!(result["count"],rows);
    let branch=result["branch"].as_u64().unwrap() as u16;
    let started=Instant::now();drop(node);let node=Node::open(&root)?;let reopen_ms=started.elapsed().as_secs_f64()*1000.;
    let started=Instant::now();let mut cursor=node.query(branch,from,from+day,Query::default())?;let mut observed=0;let mut checksum=0u128;
    loop{let page=cursor.fetch(1000)?;if page.is_empty(){break;}observed+=page.len();for object in page.objects()? { for (name,value) in object {if name=="duration" {checksum+=u128::try_from(i128::from(value.as_integer().ok_or("Expected duration integer")?))?;}else{std::hint::black_box(value);}}}}drop(cursor);assert_eq!(observed,rows);
    assert_eq!(checksum,(0..rows).map(|n|(n%20_000) as u128).sum::<u128>());
    let scan_ms=started.elapsed().as_secs_f64()*1000.;
    let started=Instant::now();let report=node.summaries(&[branch],from,from+day,300_000,"duration",&Predicate::All)?;
    assert_eq!(report["summary"]["count"],rows);let analytics_ms=started.elapsed().as_secs_f64()*1000.;
    println!("{}",serde_json::json!({"rows":rows,"input_bytes":bytes,"from":from,"to":from+day,"install_ms":install_ms,"generation_ms":generation_ms,"upload_ms":upload_ms,"parse_encode_durable_publish_ms":import_ms,"reopen_ms":reopen_ms,"all_rows_native_decode_ms":scan_ms,"analytics_ms":analytics_ms}));
    Ok(())
}
