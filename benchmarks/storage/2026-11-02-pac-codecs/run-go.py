from pathlib import Path
import subprocess,json
root=Path('/tmp/trunk-pac-codec-compare');out=Path('benchmarks/storage/2026-11-02-pac-codecs/go-results.jsonl')
with out.open('w') as f:
 for ds in ['observed','dense','sparse']:
  for level in [1,6]:
   subprocess.run([str(root/'go-baseline'),str(root/'data'/ds),str(level),'prepare'],check=True)
   for op in ['encode','decode']:
    for repeat in range(3):
     r=json.loads(subprocess.check_output([str(root/'go-baseline'),str(root/'data'/ds),str(level),op]));r['repeat']=repeat;f.write(json.dumps(r)+'\n');f.flush()
 print('Go baseline complete')
