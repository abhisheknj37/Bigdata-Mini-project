#!/usr/bin/env python3
"""Run the content aggregation locally to validate the Hadoop Streaming logic."""
import csv
from collections import defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
SOURCE = ROOT / "data" / "sample" / "staging_viewing_activity.csv"
OUTPUT = ROOT / "data" / "output" / "content_metrics.tsv"

def main():
    totals = defaultdict(lambda: [0, 0, 0])
    accepted = rejected = 0
    with SOURCE.open(newline="", encoding="utf-8-sig") as stream:
        for row in csv.DictReader(stream):
            try:
                content_id = row["content_id"].strip()
                seconds = int(float(row["watch_duration_sec"] or 0))
                buffers = int(float(row["buffer_count"] or 0))
                if not content_id or seconds < 0 or buffers < 0:
                    raise ValueError("invalid measure")
                totals[content_id][0] += 1
                totals[content_id][1] += seconds
                totals[content_id][2] += buffers
                accepted += 1
            except (KeyError, TypeError, ValueError):
                rejected += 1
    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    with OUTPUT.open("w", encoding="utf-8", newline="") as out:
        for content_id, values in sorted(totals.items()):
            out.write(f"{content_id}\t{values[0]}\t{values[1]}\t{values[2]}\n")
    print(f"Source: {SOURCE}")
    print(f"Accepted records: {accepted}")
    print(f"Rejected records: {rejected}")
    print(f"Content groups: {len(totals)}")
    print(f"Output: {OUTPUT}")

if __name__ == "__main__":
    main()
