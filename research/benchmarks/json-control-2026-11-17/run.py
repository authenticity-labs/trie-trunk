"""Sequential, interleaved native runs; preserve every attempt, including failures."""
from pathlib import Path
import subprocess
import sys
import json

out=Path(sys.argv[1] if len(sys.argv)>1 else '/tmp/json-comparison')
out.mkdir(exist_ok=True)
for i,mode in enumerate(['binary','json','json-http','json-http','json','binary','json','binary','json-http','json-http','binary','json']):
    Path('/tmp/snoot-json-mode').write_text(mode)
    report=out/f'{i}-{mode}.json'
    if report.exists(): raise RuntimeError(f'Refusing to replace {report}')
    cmd=[sys.executable,'desktop/tests/test_search.py','/tmp/snoot-json-control/Snoot.app',
         '--socket','/tmp/snoot-json.sock','--direct-socket','--dispatch','port',
         '--scope','demo/lab/indexed/sample','--from','2026-11-15T18:00:00Z',
         '--to','2026-11-15T20:00:00Z','--query','events | where duration >= 10000',
         '--expect-min-matches','100','--repeat','2','--trace','--output',str(report)]
    with report.with_suffix('.log').open('w') as log:
        result=subprocess.run(cmd,stdout=log,stderr=subprocess.STDOUT)
    print(i,mode,'exit',result.returncode,flush=True)
    if report.exists():
        data=json.loads(json.loads(report.read_text())['native'])
        for r in data.get('runs',[]):
            b=r['boundary'];c=b['steps']['trace']['counts']
            print('DOM',r.get('domReadyMs'),'wall',r['wallMs'],'total',b['totalMs'],'worker',b['workerMs'],
                  'matched',c.get('matched'),'pageHash',c.get('pageHash'),flush=True)
