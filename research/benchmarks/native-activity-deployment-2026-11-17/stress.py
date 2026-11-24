"""Repeat the pending-worker comparison; only the activity lookup differs."""
import json
import os
from pathlib import Path
import subprocess
import sys

root = Path(__file__).resolve().parents[3]
web = Path(sys.argv[1]).resolve()
out = Path(sys.argv[2]).resolve()
out.mkdir(exist_ok=True)
env = dict(os.environ)
for key in ['DYLD_INSERT_LIBRARIES', 'SNOOT_SCHEDULING', 'SNOOT_TEST_ACTIVITY']:
    env.pop(key, None)
base = [sys.executable, str(root/'desktop/tests/test_search.py'), str(root/'desktop/dist/Snoot.app'),
        '--test-web-dir', str(web), '--socket', str(root/'caches/.trunk/search.sock'),
        '--direct-socket', '--scope', 'demo/lab/indexed/sample',
        '--from', '2026-11-15T18:00:00Z', '--to', '2026-11-15T20:00:00Z',
        '--query', 'events | where duration >= 10000', '--expect-min-matches', '100', '--trace']
for name, flags in [('enabled', []), ('disabled', ['--without-activity'])]:
    report = out/(name+'.json')
    if report.exists():
        raise RuntimeError(f'Refusing to overwrite {report}')
    with (out/(name+'.log')).open('w') as log:
        run = subprocess.run(base+flags+['--output', str(report)], cwd=root, env=env,
                             stdout=log, stderr=subprocess.STDOUT)
    saved = json.loads(report.read_text())
    native = json.loads(saved['native']) if saved.get('native') else {}
    print(name, 'exit', run.returncode, 'ok', native.get('ok'), 'DOM', native.get('domReadyMs'),
          'activity', native.get('nativeActivityState'), flush=True)
    if name == 'enabled':
        assert run.returncode == 0 and native.get('ok'), saved
        counts = native['boundary']['steps']['trace']['counts']
        assert (counts['scanned'], counts['matched'], native['loaded']) == (20000, 9408, 100)
        activity = native['nativeActivityState']
        assert activity['starts'] == activity['ends'] and not any(activity[k] for k in ['active', 'pending', 'errors'])
    # A passing control must be retained and reported, never discarded.
