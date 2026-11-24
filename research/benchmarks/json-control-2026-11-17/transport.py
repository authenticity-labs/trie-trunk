"""Measure complete published bodies through the same temporary Unix API."""
import http.client
import socket
import time
import json
from pathlib import Path

old=json.loads(Path('research/benchmarks/publication-io-2026-11-17/publication-indexed-socket-bound.json').read_text())['packs']
out={'binary':[],'json':[]}
for _ in range(8):
 for mode in out:
    c=http.client.HTTPConnection('localhost',timeout=30)
    c.sock=socket.socket(socket.AF_UNIX,socket.SOCK_STREAM)
    start=time.perf_counter();c.sock.connect('/tmp/snoot-json.sock')
    c.request('GET',old['target'] if mode=='binary' else '/caches?__bench=events',headers=old['headers'] if mode=='binary' else {})
    r=c.getresponse();body=r.read();elapsed=(time.perf_counter()-start)*1000;c.close()
    assert r.status==200
    out[mode].append({'ms':elapsed,'bytes':len(body)})
Path('/tmp/json-socket.json').write_text(json.dumps(out,indent=2))
