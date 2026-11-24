#!/usr/bin/env python3
"""Repeat packaged-app queries sequentially; preserve failures and exact inputs."""
import argparse
import hashlib
import json
import platform
import statistics
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
CASES = [
    ('selective', 'SELECT CAST(duration AS BIGINT) AS time FROM events WHERE sequence = 1', ['--expect-table-total', '34'], 2),
    ('events', 'events | where duration >= "10000"', ['--expect-min-matches', '100'], 100),
    ('counts', 'SELECT bucket, SUM(count) AS total FROM summaries GROUP BY bucket', ['--expect-table-total', '20000'], 24),
    ('members', 'SELECT MIN("duration.min") AS minimum, MAX("duration.max") AS maximum FROM summaries GROUP BY bucket', [], 24),
    ('zero', 'events | where duration > "20000"', ['--expect-summary-pruned'], None),
]


def fingerprint(paths):
    result = {}
    for p in sorted(paths):
        if p.is_file():
            with p.open('rb') as source:
                digest = hashlib.file_digest(source, 'sha256').hexdigest()
            result[str(p.relative_to(ROOT) if p.is_relative_to(ROOT) else p)] = {'bytes': p.stat().st_size, 'sha256': digest}
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--rounds', type=int, default=3)
    parser.add_argument('--app', type=Path, default=ROOT/'desktop/dist/Snoot.app')
    parser.add_argument('--socket', default=str(ROOT/'caches/.trunk/search.sock'))
    args = parser.parse_args()
    if args.rounds < 1:
        parser.error('--rounds must be positive')
    args.output.mkdir(parents=True, exist_ok=False)
    dataset = ROOT/'caches/demo/lab/indexed'
    before = fingerprint(dataset.rglob('*'))
    manifest = {'platform': platform.platform(), 'scope': 'demo/lab/indexed/sample',
                'from': '2026-11-15T18:00:00Z', 'to': '2026-11-15T20:00:00Z',
                'dataset': before, 'artifacts': fingerprint([
                    args.app/'Contents/MacOS/snoot', args.app/'Contents/MacOS/neutralino', ROOT/'trunk', ROOT/'trunk-search',
                    *(args.app/'Contents/Resources/web').rglob('*')]),
                'cases': CASES, 'rounds': args.rounds,
                'method': 'Fresh disposable replica each session, three submissions; OS caches not evicted. Serial sessions. No dispatch overrides, delays, keepalive, or trace instrumentation.'}
    (args.output/'manifest.json').write_text(json.dumps(manifest, indent=2)+'\n')
    sessions = []
    for round_ in range(args.rounds):
        for name, query, checks, expected in CASES:
            label = f'{round_ + 1}-{name}'
            report = args.output/f'{label}.json'
            command = [sys.executable, str(ROOT/'desktop/tests/test_search.py'), str(args.app),
                       '--socket', args.socket, '--direct-socket', '--fresh-replica',
                       '--scope', manifest['scope'], '--from', manifest['from'], '--to', manifest['to'],
                       '--query', query, '--repeat', '3', '--activity-check', '--output', str(report), *checks]
            print(label, flush=True)
            failure = None
            with (args.output/f'{label}.log').open('w') as log:
                try:
                    code = subprocess.run(command, cwd=ROOT, stdout=log, stderr=subprocess.STDOUT, timeout=100).returncode
                except subprocess.TimeoutExpired:
                    code, failure = 124, 'Harness exceeded 100 seconds'
            entry = {'case': name, 'round': round_ + 1, 'exit': code, 'command': command}
            if failure:
                entry['error'] = failure
            if report.exists():
                raw = json.loads(report.read_text())
                messages = []
                for line in raw.get('native', '').splitlines():
                    try:
                        messages.append(json.loads(line))
                    except ValueError:
                        pass
                result = next((m for m in reversed(messages) if m.get('ok') and ('runs' in m or m.get('summaryPruned'))), None)
                if result:
                    entry['result'] = result
                    entry['valid'] = code == 0 and (result.get('summaryPruned') if expected is None else
                        len(result['runs']) == 3 and all(r['loaded'] == expected and r['domReadyMs'] is not None for r in result['runs']))
            sessions.append(entry)
            (args.output/'sessions.json').write_text(json.dumps(sessions, indent=2)+'\n')
    summary = {}
    for name, *_ in CASES:
        valid = [s['result'] for s in sessions if s['case'] == name and s.get('valid')]
        if name == 'zero':
            summary[name] = {'passed': len(valid), 'note': 'Preview rejection, not an executed zero-result scan.'}
            continue
        cold = [r['runs'][0]['domReadyMs'] for r in valid]
        warm = [v['domReadyMs'] for r in valid for v in r['runs'][1:]]
        summary[name] = {key: {'n': len(values), 'medianMs': statistics.median(values), 'minMs': min(values), 'maxMs': max(values)}
                         for key, values in [('cold', cold), ('warm', warm)] if values}
    unchanged = before == fingerprint(dataset.rglob('*'))
    summary['datasetUnchanged'] = unchanged
    summary['failedSessions'] = [f"{s['round']}-{s['case']}" for s in sessions if not s.get('valid')]
    (args.output/'summary.json').write_text(json.dumps(summary, indent=2)+'\n')
    print(json.dumps(summary, indent=2))
    return int(not unchanged or bool(summary['failedSessions']))


if __name__ == '__main__':
    sys.exit(main())
