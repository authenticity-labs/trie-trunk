use data_seeds::{registration::Trunk, summaries::SummaryCalendar};
fn main()->Result<(),Box<dyn std::error::Error>>{
 let root=std::path::PathBuf::from(std::env::args().nth(1).unwrap());
 let dest=std::path::PathBuf::from(std::env::args().nth(2).unwrap());std::fs::create_dir_all(&dest)?;
 let bytes=std::fs::read(root.join("day.source.rx"))?;
 let document=data_seeds::doc::read(&bytes)?;
 let (taxonomy,_)=trie_trunk_taxonomy::Trunk::new("Live summaries")?.registration(trie_trunk_taxonomy::registration::Registration {
  cache:"demo",service:"lab",resource:"measurements",resource_type:"sample",level:"",file:"summary-hour.rx"
 },&document)?;
 let mut node=Trunk::open(taxonomy.bytes())?;node.prescribe_file("summary-hour.rx",&bytes)?;
 let scope=node.trie().branches().select([1]).summaries()?;let start=1_767_225_600_000;
 for (name,values) in [("before",vec![100]),("changed",vec![20_000]),("appended",vec![100,20_000])]{
  let mut live=scope.accumulator_day(start,SummaryCalendar::new(0)?)?;
  for (i,value) in values.into_iter().enumerate(){let time=start+i as u64;
   let _event=live.ingest(1,time,value,|value,seed|{seed.set_uint("time",time)?.set_uint("duration",value)?.set_enum("status",200.into())?.set_text_string("message","freshness")?;Ok(())})?;
  }
  let (registry,definition,bytes)=live.snapshot_registered(&taxonomy,"summary-hour-summaries.rx")?;
  std::fs::write(dest.join(format!("{name}.pac")),bytes)?;
  std::fs::write(dest.join("trunk.trie"),registry.bytes())?;
  std::fs::write(dest.join("summary.rx"),definition.bytes()?)?;
 }
 Ok(())
}
