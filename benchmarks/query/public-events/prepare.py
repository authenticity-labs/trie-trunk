"""Extract a small repeatable projection from the three public source files."""
import argparse
from collections import Counter
from datetime import datetime, timezone
import gzip
import hashlib
import json
from pathlib import Path
import re


def github(path):
    with gzip.open(path, "rt") as source:
        for line in source:
            e = json.loads(line)
            issue = e["payload"].get("issue", {})
            yield dict(time=int(datetime.fromisoformat(e["created_at"].replace("Z", "+00:00")).timestamp()) * 1000,
                       kind=e["type"], source_id=e["id"], repo=e["repo"]["id"], issue=issue.get("id"))


def usgs(path):
    for e in json.loads(path.read_bytes())["features"]:
        p = e["properties"]
        yield dict(time=p["time"], kind=p["net"], source_id=e["id"],
                   magnitude=p["mag"], depth=e["geometry"]["coordinates"][2])


def nasa(path):
    # Take all July 1 requests. Keep no host, URL or request text.
    pattern = re.compile(rb'\[(01/Jul/1995):(\d\d):(\d\d):(\d\d) -0400\].*"\s+(\d{3})\s+(\d+|-)\s*$')
    midnight = int(datetime(1995, 7, 1, 4, tzinfo=timezone.utc).timestamp())
    with gzip.open(path, "rb") as source:
        for i, line in enumerate(source, 1):
            if b"[01/Jul/1995:" not in line:
                continue
            m = pattern.search(line)
            if not m:
                raise ValueError(f"Unparsed July 1 request on line {i}")
            h, minute, s, status, size = m.groups()[1:]
            yield dict(time=(midnight + int(h)*3600 + int(minute)*60 + int(s))*1000,
                       source_id=str(i), kind=status.decode(), size=None if size == b"-" else int(size))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("output", type=Path)
    for name in ("github", "usgs", "nasa"):
        parser.add_argument("--" + name, type=Path, required=True)
    args = parser.parse_args()
    args.output.mkdir(exist_ok=True, parents=True)
    for name, extract in (("github", github), ("usgs", usgs), ("nasa", nasa)):
        source = getattr(args, name)
        rows = sorted(extract(source), key=lambda r: r["time"])
        assert rows and len({r["source_id"] for r in rows}) == len(rows)
        path = args.output / (name + ".json")
        path.write_text(json.dumps(rows, separators=(",", ":"), allow_nan=False))
        print(name, len(rows), hashlib.sha256(source.read_bytes()).hexdigest(),
              "projection", hashlib.sha256(path.read_bytes()).hexdigest(), Counter(r["kind"] for r in rows))
        if name == "github":
            keys = lambda kind: Counter((r["repo"], r["issue"]) for r in rows if r["kind"] == kind and r["issue"] is not None)
            issues, comments = keys("IssuesEvent"), keys("IssueCommentEvent")
            shared = issues.keys() & comments.keys()
            print("issue/comment shared keys, joined pairs:", len(shared), sum(issues[k]*comments[k] for k in shared))


if __name__ == "__main__":
    main()
