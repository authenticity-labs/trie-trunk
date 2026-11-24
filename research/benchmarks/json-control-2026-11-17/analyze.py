import json
from pathlib import Path
from statistics import median

root=Path(__file__).resolve().parent
out=[]
for series in ['json-comparison','json-comparison-dom','json-comparison-http']:
 for path in sorted((root/series).glob('*.json')):
    native=json.loads(json.loads(path.read_text())['native'])
    assert native['ok'],path
    for i,r in enumerate(native['runs']):
        b=r['boundary'];s=b['steps'];t=s['trace']['times'];c=s['trace']['counts'];d=s['dispatch'];ui=b['client']
        assert c['scanned']==20000 and c['matched']==9408 and c['pageHash']==2304089401,(path,c)
        assert r['loaded']==100 and r['visible']>0,path
        out.append(dict(series=series,file=path.name,mode=path.stem.split('-',1)[1],state='first' if i==0 else 'repeat',
            dom=r.get('domReadyMs'),observed=r['wallMs'],total=b['totalMs'],worker=b['workerMs'],
            outside=b['outsideWorkerMs'],request=d['nodeReceivedAt']-d['clientSentAt'],
            forward=ui['view-summaryAt']-d['forwardedAt'],page=ui['pageMs'],adapt=ui['convertMs'],
            collection=s.get('replicationMs'),times=t,counts=c))
(root/'measurements.json').write_text(json.dumps(out,indent=2))
for series in ['json-comparison','json-comparison-dom','json-comparison-http']:
 print(series)
 for mode in ['binary','json','json-http']:
  for state in ['first','repeat']:
    selected=[r for r in out if r['series']==series and r['mode']==mode and r['state']==state]
    if not selected:continue
    print(mode,state,{k:median([r[k] for r in selected if r[k] is not None]) for k in ['observed','total','worker','outside','request','forward','page','adapt','collection']},
      'DOM',median([r['dom'] for r in selected]) if selected[0]['dom'] is not None else None)
    print('stages',{k:median([r['times'].get(k,0)for r in selected])for k in ['control.mode','json.response','json.body','json.parse','json.filterSort','readerOpen','poll','materialize']})
