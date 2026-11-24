"""Sequential, interleaved native scheduling comparison; preserve every failure."""
from pathlib import Path
import subprocess,json,sys,os
out=Path(sys.argv[1] if len(sys.argv)>1 else '/tmp/native-scoped-scheduling');out.mkdir(exist_ok=True)
modes=sys.argv[2:] or ['default','scoped','activity','activity','scoped','default']
for i,mode in enumerate(modes):
 report=out/f'{i}-{mode}.json'
 if report.exists():raise RuntimeError(f'Refusing to overwrite {report}')
 cmd=[sys.executable,'desktop/tests/test_search.py',os.environ.get('SNOOT_TEST_APP','/tmp/snoot-scoped-scheduling-wake/Snoot.app'),
 '--socket','/Users/Shared/code/applications/trie-trunk/caches/.trunk/search.sock','--direct-socket','--dispatch','port',
 '--scope','demo/lab/indexed/sample','--from','2026-11-15T18:00:00Z','--to','2026-11-15T20:00:00Z',
 '--query','events | where duration >= 10000','--expect-min-matches','100','--repeat',os.environ.get('SNOOT_TEST_REPEAT','10'),'--native-wake','--trace','--activity-check','--output',str(report)]
 if os.environ.get('SNOOT_RECORD_WARNINGS'):cmd.append('--record-resize-warnings')
 if os.environ.get('SNOOT_ACTIVITY_CANCEL'):cmd.append('--activity-cancel')
 if os.environ.get('SNOOT_RESIZE_TRACE'):cmd.append('--resize-trace')
 if os.environ.get('SNOOT_WORKER_DELAY'):cmd.extend(['--worker-delay',os.environ['SNOOT_WORKER_DELAY']])
 env=dict(os.environ,DYLD_INSERT_LIBRARIES='/tmp/snoot-scoped-activity-wake.dylib',SNOOT_SCHEDULING=mode)
 env.pop('SNOOT_TEST_ACTIVITY',None)
 with report.with_suffix('.log').open('w')as log:r=subprocess.run(cmd,stdout=log,stderr=subprocess.STDOUT,env=env)
 print(i,mode,'exit',r.returncode,flush=True)
 if not report.exists():continue
 saved=json.loads(report.read_text());raw=saved.get('native');data=json.loads(raw)if raw else {}
 print('host',saved.get('host'),'activity',data.get('activity'),'error',data.get('error'),flush=True)
 for line in report.with_suffix('.log').read_text().splitlines():
  if not line.startswith('Native Progress: '):continue
  try:progress=json.loads(line[len('Native Progress: '):])
  except ValueError:continue
  if progress.get('stage')!='complete':continue
  b=progress['boundary'];c=b['steps']['trace']['counts']
  assert c['scanned']==20000 and c['matched']==9408 and progress['loaded']==100
  print(progress['repeat'],'DOM',progress.get('domReadyMs'),'worker',b['workerMs'],flush=True)
