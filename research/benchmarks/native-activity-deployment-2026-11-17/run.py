"""Verify queries in the deployed app, with no injected native library."""
import json
import os
from pathlib import Path
import statistics
import subprocess
import sys

root=Path(__file__).resolve().parents[3]
out=Path(sys.argv[1]);out.mkdir(exist_ok=True)
env=dict(os.environ)
for key in ['DYLD_INSERT_LIBRARIES','SNOOT_SCHEDULING','SNOOT_TEST_ACTIVITY']:env.pop(key,None)
base=[sys.executable,str(root/'desktop/tests/test_search.py'),str(root/'desktop/dist/Snoot.app'),
 '--socket',str(root/'caches/.trunk/search.sock'),'--direct-socket','--scope','demo/lab/indexed/sample',
 '--from','2026-11-15T18:00:00Z','--to','2026-11-15T20:00:00Z','--trace']
failed=False
for name,extra in [(f'queries-{i}',['--query','events | where duration >= 10000','--expect-min-matches','100','--repeat','10','--activity-check','--record-resize-warnings'])for i in range(3)]+[
 ('summary-pruning',['--query','events | where duration > 4000000000','--expect-summary-pruned'])]:
 report=out/(name+'.json')
 if report.exists():raise RuntimeError(f'Refusing to overwrite {report}')
 with (out/(name+'.log')).open('w')as log:
  run=subprocess.run(base+extra+['--output',str(report)],cwd=root,env=env,stdout=log,stderr=subprocess.STDOUT)
 failed|=bool(run.returncode)
 if not report.exists():print(name,'NO REPORT',flush=True);continue
 saved=json.loads(report.read_text());result=json.loads(saved['native'])if saved.get('native')else {}
 print(name,'exit',run.returncode,'ok',result.get('ok'),'error',result.get('error'),flush=True)
 for row in result.get('runs',[]):
  counts=row['boundary']['steps']['trace']['counts'];assert (counts['scanned'],counts['matched'],row['loaded'])==(20000,9408,100)
 if result.get('runs'):
  times=[r['domReadyMs'] for r in result['runs']]
  print('DOM',times,'warm median',statistics.median(times[1:]),'warnings',len(result.get('warnings',[])),'activity',result.get('activity'),flush=True)
sys.exit(1 if failed else 0)
