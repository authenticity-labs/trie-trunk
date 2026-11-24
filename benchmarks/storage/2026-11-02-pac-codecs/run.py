import json,subprocess,random,time,platform
from pathlib import Path
root=Path('/tmp/trunk-pac-codec-compare')
binary=root/'rust/target/release/pac-codec-compare'
out=Path('benchmarks/storage/2026-11-02-pac-codecs')
cases=[('gzip',1),('gzip',6),('gzip',9),('zstd',1),('zstd',3),('zstd',9),('zstd',19),('br',1),('br',5),('br',9),('br',11)]
for dataset in ['observed','dense','sparse']:
 for codec,level in cases:
  subprocess.run([str(binary),str(root/'data'/dataset),codec,str(level),'prepare'],check=True)
 print('Prepared and verified',dataset,flush=True)
jobs=[(dataset,codec,level,op,repeat) for dataset in ['observed','dense','sparse'] for codec,level in cases for op in ['encode','decode'] for repeat in range(3)]
random.Random(742).shuffle(jobs)
with (out/'results.jsonl').open('w') as f:
 for i,(dataset,codec,level,op,repeat) in enumerate(jobs):
  result=json.loads(subprocess.check_output([str(binary),str(root/'data'/dataset),codec,str(level),op]))
  result['repeat']=repeat;f.write(json.dumps(result)+'\n');f.flush()
  if i%20==0:print('Measured',i+1,'/',len(jobs),flush=True)
(out/'environment.json').write_text(json.dumps({'platform':platform.platform(),'machine':platform.machine(),'rustc':subprocess.check_output(['rustc','--version'],text=True).strip(),'method':'Single-thread codec operations, 3 fresh processes per case, at least 200ms measured per process, one warmup, no disk/network in timed region. macOS ru_maxrss is total peak resident process bytes.'},indent=2)+'\n')
