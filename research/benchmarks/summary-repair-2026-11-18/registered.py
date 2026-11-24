import hashlib, json, statistics, subprocess
from pathlib import Path
out = Path(__file__).resolve().parent
bins = {"baseline": Path("/tmp/summary-final-bin/after"), "cleanup": Path("rust/seeds/target/release/examples/summary_collection").resolve()}
results = []
for case in ["narrow", "typical", "wide", "quiet"]:
    for binary in bins.values(): subprocess.check_output([str(binary), case])
    pairs = []
    for i in range(9):
        pair = {}
        for name in (["baseline", "cleanup"] if i % 2 == 0 else ["cleanup", "baseline"]):
            pair[name] = json.loads(subprocess.check_output([str(bins[name]), case]))
        for key in ["events", "days", "event_sha256"]:
            assert pair["baseline"][key] == pair["cleanup"][key], (case, key)
        assert pair["cleanup"]["bytes"] == pair["baseline"]["bytes"] - 29 * pair["baseline"]["days"]
        pairs.append(pair)
    medians = {key: {v: statistics.median(p[v][key] for p in pairs) for v in bins} for key in ["open_us", "cold_query_us", "cold_total_us", "warm_query_us"]}
    results.append({"case": case, "medians": medians, "pairs": pairs})
    print(case, medians, flush=True)
    (out / "registered-results.json").write_text(json.dumps(results, indent=2) + "\n")
(out / "registered-manifest.json").write_text(json.dumps({
    "binaries": {v: hashlib.sha256(b.read_bytes()).hexdigest() for v,b in bins.items()},
    "baseline_source": "research/benchmarks/summary-completion-2026-11-17/source.tar.gz",
    "scope": "Native release resident daily PAC open/preview. Typed event ingestion creates the fixture outside read timings. The only expected encoded PAC size change is removal of the 29-byte summary.definitions application header. Dataset, coverage and exact zero result are checked in both executables. No disk/network/worker/UI timings."
}, indent=2) + "\n")
