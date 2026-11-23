#!/usr/bin/env python3
"""Hadoop Streaming mapper: emit per-content streaming metrics from the raw CSV."""
import csv
import sys

for line in sys.stdin:
    try:
        row = next(csv.reader([line]))
        if not row or row[0].strip().lower() == "viewing_id":
            continue
        # Existing schema: content_id=column 4, watch_duration_sec=column 9,
        # buffer_count=column 12 (1-based).
        if len(row) < 12:
            continue
        content_id = row[3].strip()
        watched_seconds = int(float(row[8] or 0))
        buffering_events = int(float(row[11] or 0))
        if content_id:
            print(f"{content_id}\t1\t{watched_seconds}\t{buffering_events}")
    except (ValueError, IndexError, csv.Error):
        continue
