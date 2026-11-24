"""Diagnostic sampling, excluded from latency comparisons."""
import subprocess,time
from pathlib import Path
def contents():
    lines=subprocess.check_output(['ps','-axo','pid,comm'],text=True).splitlines()
    return {int(l.split()[0])for l in lines if 'com.apple.WebKit.WebContent.xpc/Contents/MacOS/com.apple.WebKit.WebContent' in l}
before=contents()
cmd=['python3','desktop/tests/test_search.py','/tmp/snoot-delivery-before/Snoot.app',
 '--socket','/Users/Shared/code/applications/trie-trunk/caches/.trunk/search.sock','--direct-socket',
 '--scope','demo/lab/indexed/sample','--from','2026-11-15T18:00:00Z','--to','2026-11-15T20:00:00Z',
 '--query','events | where duration >= 10000','--expect-min-matches','100','--repeat','12','--trace',
 '--output','/tmp/delivery-profile.json']
with open('/tmp/delivery-profile.log','w')as log:
    p=subprocess.Popen(cmd,stdout=log,stderr=subprocess.STDOUT)
    try:
      deadline=time.monotonic()+20
      while time.monotonic()<deadline:
        created=contents()-before
        if created:
          if len(created)!=1:raise RuntimeError('Ambiguous WebContent process; not sampling')
          time.sleep(2)
          subprocess.run(['sample',str(next(iter(created))),'6','2','-file','/tmp/delivery-webcontent-sample.txt'],check=True)
          break
        if p.poll()is not None:raise RuntimeError('Test exited before WebContent appeared')
        time.sleep(.2)
      print('Test exit',p.wait(timeout=70))
    finally:
      if p.poll()is None:p.terminate();p.wait()
