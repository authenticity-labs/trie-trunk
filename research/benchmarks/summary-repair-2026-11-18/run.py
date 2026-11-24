#!/usr/bin/env python3
"""Run the same native harness against both frozen implementations."""
import hashlib,json,platform,statistics,subprocess,time
from pathlib import Path
OUT=Path(__file__).resolve().parent
BIN=Path('/tmp/summary-repair-bin')
CASES=['narrow','typical','wide','quiet']
results=[]
for case in CASES:
    # Warm executable pages and CPU before recorded pairs; each run still
    # creates fresh events, plans, accumulators, encoded Packs and readers.
    for version in ['baseline','cleanup']:
        subprocess.run([str(BIN/version),case],check=True,stdout=subprocess.DEVNULL)
    rows=[]
    for pair in range(9):
        order=['baseline','cleanup'] if pair%2==0 else ['cleanup','baseline']
        samples={}
        for version in order:
            samples[version]=json.loads(subprocess.check_output([str(BIN/version),case]))
        for key in ['event_sha256','decoded_sha256','dashboard_sha256','events','category','event_bytes','summary_rows']:
            assert samples['baseline'][key]==samples['cleanup'][key],(case,pair,key)
        if rows:
            for version in order:
                for key in ['event_sha256','decoded_sha256','summary_box_bytes','heap']:
                    assert samples[version][key]==rows[0][version][key],(case,pair,version,key)
        rows.append({'pair':pair,'order':order,**samples})
    comparison={}
    for key in rows[0]['baseline']['timing']:
        p=[r['baseline']['timing'][key] for r in rows];c=[r['cleanup']['timing'][key] for r in rows]
        comparison[key]={'prior_median_us':statistics.median(p),'current_median_us':statistics.median(c),
            'change_percent':(statistics.median(c)/statistics.median(p)-1)*100,
            'prior_min_max_us':[min(p),max(p)],'current_min_max_us':[min(c),max(c)]}
    results.append({'case':case,'comparison':comparison,'pairs':rows})
    (OUT/'results.json').write_text(json.dumps(results,indent=2)+'\n')
    print(case,json.dumps({k:v for k,v in comparison.items() if k in ['build_total_us','cold_zero_total_us','warm_zero_preview_us']}),flush=True)
manifest={'platform':platform.platform(),'machine':platform.machine(),'timestamp':time.strftime('%Y-%m-%dT%H:%M:%S%z'),
 'rustc':subprocess.check_output(['rustc','-Vv'],text=True),
 'binaries':{v:hashlib.sha256((BIN/v).read_bytes()).hexdigest() for v in ['baseline','cleanup']},
 'scope':'Native release Rust summary APIs on resident byte buffers. No disk/network/UI, SQL parsing or cross-worker transfer. Mixed-definition exception behavior excluded.',
 'heap':'Separate untimed allocation pass. Incremental live requested heap, not RSS; existing input/Definition allocations excluded. Cold reader includes copying Box input.',
 'baseline':'Preserved optimized array-summary executable from 2026-11-17; source archived in summary-arrays-optimized-2026-11-17/current-source.tar.gz. Cleanup removes obsolete generator/preview dispatch and redundant PAC references; width-specific class matrix remains. This is not certification of the planned schema replacement.',
 'pairs':9,'warmups_per_case_per_binary':1}
(OUT/'manifest.json').write_text(json.dumps(manifest,indent=2)+'\n')
