//! End-to-end selected-result conversion versus repeated lazy result access.
use data_seeds::*;
use pil_codec::{Value,api::NativeValue};
use std::{hint::black_box,time::Instant};
use serde_json::json;
fn fixtures(variable:bool)->(Beds,usize) {
    let mut doc=doc::new("stalk-ingress-benchmark");
    for i in 0..32 {doc=doc.field(Field::new(format!("f{i}")).r#type("float").size(8));}
    if variable {doc=doc.field(Field::new("message").r#type("tstr"));}
    let def=SeedDefinition::prescribe(&doc.sign().unwrap()).unwrap();let mut beds=Beds::new();let mut bytes=0;
    for first in (0..10_000).step_by(2500) {
        let mut pack=def.fill(2500).unwrap();
        for n in first..first+2500 {
            let message=format!("event {n}: {}", "changing payload ".repeat(n%8+1));
            let seed=def.ingest(n,|n,s|{
                for i in 0..32 {s.set_float(i,((n*17+i*23)%10007) as f64+0.25)?;}
                if variable {s.set_text_string("message",&message)?;}
                Ok(())
            }).unwrap();pack.load(seed).unwrap();
        }
        pack.seal().unwrap();let pack=def.open(pack.into_bytes().unwrap()).unwrap();bytes+=pack.byte_length();beds.pack(pack);
    }
    (beds,bytes)
}
fn measure<T>(mut run:impl FnMut()->T)->serde_json::Value {
    for _ in 0..2 {black_box(run());}let mut times=Vec::new();
    for _ in 0..7 {let start=Instant::now();for _ in 0..5 {black_box(run());}times.push(start.elapsed().as_secs_f64()*1000.0/5.0);}
    let mut sorted=times.clone();sorted.sort_by(f64::total_cmp);json!({"median_ms":sorted[3],"samples_ms":times})
}
fn main() {
    let mut results=Vec::new();
    for variable in [false,true] {
        let (beds,bytes)=fixtures(variable);
        let grow=||{let budget=MemoryBudget::new(8*1024*1024);let mut growth=beds.query(Query::new().select(["f0"])).unwrap().stalks(budget,10_000);while !growth.poll(8192).unwrap() {}growth.finish().unwrap()};
        let stalk=grow();let copied=stalk.bytes();
        let Cell::Value(ciborium::Value::Float(expected))=stalk.sum("f0").unwrap() else {panic!()};drop(stalk);
        let mut cases=Vec::new();
        for passes in [1,10,100] {
            let row=||{
                let mut cursor=beds.query(Query::new().select(["f0"])).unwrap();let batch=cursor.fetch(10_000).unwrap();let mut sum=0.0;
                for _ in 0..passes {let mut reader=batch.reader();for i in 0..batch.len(){let row=reader.view(i).unwrap();let NativeValue::Scalar(Value::F64(v))=row.native_at(0).unwrap() else {panic!()};sum+=v;}}
                sum
            };
            let column=||{let stalk=grow();let mut total=0.0;for _ in 0..passes{let Cell::Value(ciborium::Value::Float(v))=black_box(&stalk).sum("f0").unwrap() else{panic!()};total+=v;}total};
            assert_eq!(row(),expected*passes as f64);assert_eq!(column(),expected*passes as f64);
            cases.push(json!({"passes":passes,"row":measure(row),"grow_and_reduce":measure(column)}));
        }
        results.push(json!({"variable":variable,"rows":10_000,"fields":if variable{33}else{32},"source_bytes":bytes,"column_bytes":copied,"growth":measure(grow),"cases":cases}));
    }
    println!("{}",serde_json::to_string_pretty(&results).unwrap());
}
