import hashlib,json,statistics,subprocess
from pathlib import Path
out=Path(__file__).resolve().parent
bins=Path('/tmp/summary-context-bin')
results=[]
for case in ['narrow','typical','wide','quiet']:
 for version in ['prior','current']:subprocess.check_output([str(bins/version),case])
 pairs=[]
 for i in range(9):
  pair={}
  for version in (['prior','current'] if i%2==0 else ['current','prior']):pair[version]=json.loads(subprocess.check_output([str(bins/version),case]))
  for key in ['events','days','event_sha256','pac_sha256','bytes']:assert pair['prior'][key]==pair['current'][key],(case,key)
  pairs.append(pair)
 medians={key:{v:statistics.median(p[v][key] for p in pairs) for v in ['prior','current']} for key in ['open_us','cold_query_us','cold_total_us','warm_query_us']}
 results.append({'case':case,'medians':medians,'pairs':pairs});print(case,medians,flush=True)
 (out/'results.json').write_text(json.dumps(results,indent=2)+'\n')
(out/'manifest.json').write_text(json.dumps({'binaries':{v:hashlib.sha256((bins/v).read_bytes()).hexdigest() for v in ['prior','current']},'baseline':'summary-box-context-2026-11-17/current-source.tar.gz','scope':'Native release resident daily PAC reads. Event ingestion, summary preparation and registry publication are outside these read-only timings. One clone per received PAC inside open timing; 100 warm queries per run. Nine alternating pairs. Same harness, event bytes and summary PAC bytes verified. Shared current taxonomy and codecs in both builds.'},indent=2)+'\n')
