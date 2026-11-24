"""Summarize complete profiles; retain failed/partial profiles explicitly."""
import collections
import json
import pathlib
import statistics
import sys

groups = collections.defaultdict(list)
for path in sorted(pathlib.Path(sys.argv[1]).glob('*.json')):
    saved = json.loads(path.read_text())
    report = json.loads(saved['native']) if saved.get('native') else {}
    runs = report.get('runs', [])
    mode = saved['host']['scheduling']
    if not report.get('ok'):
        print(path.name, 'FAILED', report.get('error', 'No completed report'))
        continue
    for row in runs:
        counts = row['boundary']['steps']['trace']['counts']
        assert (counts['scanned'], counts['matched'], row['loaded']) == (20000, 9408, 100)
    cold, *warm = runs
    groups[mode].extend(warm)
    print(path.name, 'first DOM/worker:', cold['domReadyMs'], cold['boundary']['workerMs'],
          'warm DOM:', [row['domReadyMs'] for row in warm],
          'warnings:', len(report.get('warnings', [])), 'activity:', report.get('activity'))

for mode, rows in groups.items():
    if not rows:
        continue
    dom = [row['domReadyMs'] for row in rows]
    worker = [row['boundary']['workerMs'] for row in rows]
    print(mode, 'warm n:', len(rows), 'DOM median/min/max:', statistics.median(dom), min(dom), max(dom),
          'worker median:', statistics.median(worker))
