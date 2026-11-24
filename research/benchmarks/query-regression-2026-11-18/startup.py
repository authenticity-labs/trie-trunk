"""Fresh-profile startup and first/repeat query timings; no injected keepalive."""
import json
import os
from pathlib import Path
import subprocess
import sys

root = Path(__file__).resolve().parents[3]
out = Path(sys.argv[1]); out.mkdir(exist_ok=True)
env = dict(os.environ)
for key in ['DYLD_INSERT_LIBRARIES', 'SNOOT_SCHEDULING', 'SNOOT_TEST_ACTIVITY']:
    env.pop(key, None)
for index in range(6):
    report = out/f'{index}.json'
    if report.exists():
        raise RuntimeError(f'Refusing to overwrite {report}')
    command = [sys.executable, str(root/'desktop/tests/test_search.py'), str(root/'desktop/dist/Snoot.app'),
               '--socket', str(root/'caches/.trunk/search.sock'), '--direct-socket', '--fresh-replica',
               '--scope', 'demo/lab/indexed/sample', '--from', '2026-11-15T18:00:00Z',
               '--to', '2026-11-15T20:00:00Z', '--query', 'events | where duration >= 10000',
               '--expect-min-matches', '100', '--repeat', '2', '--trace', '--output', str(report)]
    with (out/f'{index}.log').open('w') as log:
        run = subprocess.run(command, cwd=root, env=env, stdout=log, stderr=subprocess.STDOUT)
    if run.returncode:
        raise RuntimeError(f'Profile {index} failed; see {report.with_suffix(".log")}')
    data = json.loads(report.read_text()); result = json.loads(data['native'])
    assert result['ok']
    print(index, 'startup', result['startup'], flush=True)
    for n, row in enumerate(result['runs']):
        boundary = row['boundary']; steps = boundary['steps']; counts = steps['trace']['counts']
        assert (counts['scanned'], counts['matched'], row['loaded']) == (20000, 9408, 100)
        assert counts['collectionHours'] == (2 if n == 0 else 0), 'Unexpected replica warmth'
        client = boundary['client']; dispatch = steps['dispatch']
        print('first' if n == 0 else 'repeat', {
            'dom': row['domReadyMs'], 'port': client['portReadyAt']-client['portStartAt'],
            'broker': dispatch['nodeReceivedAt']-dispatch['clientSentAt'],
            'service': dispatch['serviceReceivedAt']-dispatch['servicePostedAt'],
            'worker': boundary['workerMs'], 'outsideWorker': boundary['outsideWorkerMs'],
            'times': steps['trace']['times'],
        }, flush=True)
    activity = result['nativeActivityState']
    assert activity['starts'] == activity['ends']
    assert not any(activity[key] for key in ['pending', 'active', 'errors'])
