use std::{fs,path::PathBuf,sync::Arc,collections::BTreeSet};
use data_seeds::{registration::{Trunk,IngressLimits,ingress::recovery},Query};
fn main()->Result<(),Box<dyn std::error::Error>>{
 let root=PathBuf::from(std::env::args().nth(1).unwrap());
 let bytes=fs::read(root.join("trunk.trie"))?;
 let registry=trie_trunk_taxonomy::Trunk::open(bytes.clone())?;
 let mut trunk=Trunk::open(bytes)?;
 let mut branches=vec![];
 for r in registry.definitions()?.resources(){let route=registry.route(r.branch)?;if route.service=="trie-trunk"&&route.resource=="queries"{branches.push(r.branch);let d=registry.definition(r.definition)?;trunk.prescribe_file(&d.file,&fs::read(root.join("definitions").join(&d.file))?)?;}}
 let scope=trunk.trie().branches().select(branches.clone()).packs()?;
 let mut ingress=scope.ingress(branches[0],IngressLimits{open_hours:100,..Default::default()},0)?;
 let bytes=fs::read(root.join("demo/trie-trunk/queries/query.seeds"))?;
 let mut at=0;let mut hours=BTreeSet::new();
 while at<bytes.len(){let n=recovery::length(&bytes[at..],1<<20)?;let(b,p)=recovery::read(&bytes[at..at+n],1<<20)?;if b!=0{if let Some(h)=ingress.recover(b,p)?{hours.insert(h/3600000*3600000);}}at+=n;}
 for hour in hours {let bytes=ingress.snapshot(hour)?;let n=bytes.len();let mut reader=scope.reader_scope();reader.load_registered(Arc::new(bytes),0..n,64<<20)?;let mut cursor=reader.query(Query::new())?;let batch=cursor.fetch(10000)?;for row in batch.objects()?{let data:std::collections::BTreeMap<_,_>=row.into_iter().filter(|(k,_)|k!="id"&&k!="user_agent").collect();println!("{}",serde_json::to_string(&data)?);}}
 Ok(())
}
