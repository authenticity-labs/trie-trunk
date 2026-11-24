#!/usr/bin/env python3
"""Same unchanged harnesses, alternating saved optimized baseline/current build."""
import hashlib,json,platform,statistics,subprocess,time
from pathlib import Path
out=Path(__file__).resolve().parent
root=out.parents[2]
configs={
 'results':({'before':Path('/tmp/summary-repair-bin/cleanup'),'after':root/'rust/seeds/target/release/examples/summary_comparison'},['event_sha256','decoded_sha256','dashboard_sha256','events','category','event_bytes','summary_rows']),
 'registered':({'before':Path('/tmp/summary-final-bin/after'),'after':root/'rust/seeds/target/release/examples/summary_collection'},['event_sha256','events','days'])}
manifest={'timestamp':time.strftime('%Y-%m-%dT%H:%M:%S%z'),'platform':platform.platform(),'scope':'Native release Rust; resident bytes; no disk/network/UI; 9 alternating pairs per case. Both harnesses encode actual typed input. Summary and registered read harnesses unchanged.','binaries':{}}
for name,(bins,checks) in configs.items():
 results=[]
 manifest['binaries'][name]={k:hashlib.sha256(v.read_bytes()).hexdigest() for k,v in bins.items()}
 for case in ['narrow','typical','wide','quiet']:
  for binary in bins.values():subprocess.check_output([str(binary),case])
  pairs=[]
  for i in range(9):
   pair={k:json.loads(subprocess.check_output([str(bins[k]),case])) for k in (['before','after'] if i%2==0 else ['after','before'])}
   for key in checks:assert pair['before'][key]==pair['after'][key],(case,key)
   pairs.append(pair)
  keys=list(pairs[0]['before']['timing']) if name=='results' else ['open_us','cold_query_us','cold_total_us','warm_query_us']
  def value(pair,k,key):return pair[k]['timing'][key] if name=='results' else pair[k][key]
  medians={key:{k:statistics.median(value(p,k,key) for p in pairs) for k in bins} for key in keys}
  results.append({'case':case,'medians':medians,'pairs':pairs})
  print(name,case,{k:v for k,v in medians.items() if k in ['build_total_us','cold_zero_total_us','warm_zero_preview_us','cold_total_us','warm_query_us']},flush=True)
  (out/(name+'.json')).write_text(json.dumps(results,indent=2)+'\n')
(out/'manifest.json').write_text(json.dumps(manifest,indent=2)+'\n')
