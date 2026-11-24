use std::{env,fs,io::{self,Read,Cursor},path::Path,time::Instant,hint::black_box};
fn encode(codec:&str,level:i32,raw:&[u8],out:&mut Vec<u8>){
 out.clear();
 match codec {
  "gzip"=>pil_codec::compression::deflate(raw,level as u32,out).unwrap(),
  "br"=>{let params=brotli::enc::BrotliEncoderParams{quality:level,..Default::default()};brotli::BrotliCompress(&mut Cursor::new(raw),out,&params).unwrap();},
  "zstd"=>zstd::stream::copy_encode(Cursor::new(raw),out,level).unwrap(),
  _=>panic!("codec")
 }
}
fn decode(codec:&str,raw:&[u8],size:usize,out:&mut Vec<u8>){
 if codec=="gzip"{pil_codec::compression::inflate(raw,size,out).unwrap();return;}
 out.resize(size,0);
 let mut reader:Box<dyn Read>=match codec{
  "br"=>Box::new(brotli::Decompressor::new(Cursor::new(raw),4096)),
  "zstd"=>Box::new(zstd::stream::read::Decoder::new(Cursor::new(raw)).unwrap()),
  _=>panic!("codec")
 };
 reader.read_exact(out).unwrap();let mut tail=[0u8];assert_eq!(reader.read(&mut tail).unwrap(),0);
}
fn usage()->(f64,i64){unsafe{let mut r:libc::rusage=std::mem::zeroed();assert_eq!(libc::getrusage(libc::RUSAGE_SELF,&mut r),0);((r.ru_utime.tv_sec+r.ru_stime.tv_sec)as f64+(r.ru_utime.tv_usec+r.ru_stime.tv_usec)as f64/1e6,r.ru_maxrss)}}
fn main()->io::Result<()>{
 let args:Vec<_>=env::args().collect();let dir=Path::new(&args[1]);let codec=&args[2];let level:i32=args[3].parse().unwrap();let op=&args[4];
 let mut paths:Vec<_>=fs::read_dir(dir)?.map(|p|p.unwrap().path()).filter(|p|p.extension().and_then(|s|s.to_str())==Some("pac")).collect();paths.sort();
 let raws:Vec<_>=paths.iter().map(|p|fs::read(p).unwrap()).collect();let mut packed=Vec::new();let mut out=Vec::with_capacity(raws.iter().map(Vec::len).max().unwrap()+1024);
 for (path,raw) in paths.iter().zip(&raws){let saved=path.with_extension(format!("{codec}-{level}"));if op=="prepare"{encode(codec,level,raw,&mut out);fs::write(&saved,&out)?;}let bytes=fs::read(saved)?;if op=="prepare"{decode(codec,&bytes,raw.len(),&mut out);assert_eq!(&out,raw);}packed.push(bytes);}
 if op=="prepare"{return Ok(());}
 let raw_bytes:usize=raws.iter().map(Vec::len).sum();let stored_bytes:usize=packed.iter().map(|v|v.len()+16).sum();let records:u64=raws.iter().map(|v|u32::from_le_bytes(v[12..16].try_into().unwrap()) as u64).sum();
 // Warm-up and byte-for-byte verification outside measurement.
 for (raw,enc) in raws.iter().zip(&packed){if op=="encode"{encode(codec,level,raw,&mut out);assert_eq!(&out,enc);}else{decode(codec,enc,raw.len(),&mut out);assert_eq!(&out,raw);}}
 let before=usage();let started=Instant::now();let mut loops=0;
 loop{
  for (raw,enc) in raws.iter().zip(&packed){if op=="encode"{encode(codec,level,raw,&mut out);}else{decode(codec,enc,raw.len(),&mut out);}black_box(&out);}
  loops+=1;if started.elapsed().as_secs_f64()>=0.2{break;}
 }
 let wall=started.elapsed().as_secs_f64();let after=usage();
 println!("{}",serde_json::json!({"dataset":dir.file_name().unwrap().to_str().unwrap(),"implementation":"rust","codec":codec,"level":level,"operation":op,"packs":raws.len(),"records":records,"raw_bytes":raw_bytes,"stored_bytes":stored_bytes,"loops":loops,"wall_ms":wall*1000.0/loops as f64,"cpu_ms":(after.0-before.0)*1000.0/loops as f64,"peak_rss_bytes":after.1}));Ok(())
}
