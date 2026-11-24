from pathlib import Path
import subprocess,json,sys
root=Path(sys.argv[1] if len(sys.argv)>1 else '/tmp/query-open-steady');root.mkdir(exist_ok=True)
for i,mode in enumerate(['before','after','after','before']):
    report=root/f'{i}-{mode}.json'
    if report.exists():raise RuntimeError(f'Refusing to overwrite {report}')
    cmd=[sys.executable,'desktop/tests/test_search.py',('/tmp/snoot-source-trace/Snoot.app' if mode=='before' else '/tmp/snoot-query-open/Snoot.app'),
      '--socket','/Users/Shared/code/applications/trie-trunk/caches/.trunk/search.sock','--direct-socket',
      '--scope','demo/lab/indexed/sample','--from','2026-11-15T18:00:00Z','--to','2026-11-15T20:00:00Z',
      '--query','events | where duration >= 10000','--expect-min-matches','100','--repeat','10','--trace','--output',str(report)]
    with report.with_suffix('.log').open('w')as log:r=subprocess.run(cmd,stdout=log,stderr=subprocess.STDOUT)
    print(i,mode,'exit',r.returncode,flush=True)
    if report.exists():
      raw=json.loads(report.read_text())['native']
      if not raw:
       print('No completed native report; retaining failure log',flush=True);continue
      data=json.loads(raw)
      for run in data.get('runs',[]):
       b=run['boundary'];c=b['steps']['trace']['counts'];assert c['scanned']==20000 and c['matched']==9408
       print('DOM',run.get('domReadyMs'),'total',b['totalMs'],'worker',b['workerMs'],'loaded',run['loaded'],flush=True)
