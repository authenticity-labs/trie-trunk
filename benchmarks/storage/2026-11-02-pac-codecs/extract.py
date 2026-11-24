# Snapshot and stratify existing PACs without changing source files.
import gzip, hashlib, json, statistics, struct
from pathlib import Path
root=Path('caches');out=Path('/tmp/trunk-pac-codec-compare/data/observed');out.mkdir(parents=True,exist_ok=True)
rows=[]
for p in sorted(root.rglob('*.pods')):
 if p.name=='write.pods':continue # Deliberately uncompressed by application policy.
 b=p.read_bytes();at=72
 while at+16<=len(b):
  size,flags,_,count=struct.unpack_from('<4I',b,at)
  assert size>=16 and at+size<=len(b),(p,at,size)
  raw=gzip.decompress(b[at+16:at+size]) if flags==2 else b[at:at+size]
  assert struct.unpack_from('<I',raw)[0]==len(raw)
  rows.append((str(p),at,count,raw));at+=size
 assert at==len(b)
# Deterministic sample plus every larger pack avoids excluding real server logs.
chosen={i for i,r in enumerate(rows) if len(r[3])>=4096}
chosen.update(sorted(range(len(rows)),key=lambda i:hashlib.sha256((rows[i][0]+str(rows[i][1])).encode()).digest())[:128])
manifest=[]
for i in sorted(chosen):
 p,offset,count,raw=rows[i];name=f'{i:05}.pac';(out/name).write_bytes(raw)
 manifest.append(dict(file=name,source=p,offset=offset,records=count,bytes=len(raw),sha256=hashlib.sha256(raw).hexdigest()))
report=dict(population_packs=len(rows),population_records=sum(r[2] for r in rows),population_raw_bytes=sum(len(r[3]) for r in rows),population_median_pac_bytes=statistics.median(len(r[3]) for r in rows),sample=manifest)
Path('benchmarks/storage/2026-11-02-pac-codecs/corpus.json').write_text(json.dumps(report,indent=2)+'\n')
print('observed sample:',len(manifest),'PACs',sum(r['records'] for r in manifest),'records',sum(r['bytes'] for r in manifest),'raw bytes')
