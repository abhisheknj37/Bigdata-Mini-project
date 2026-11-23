#!/usr/bin/env python3
"""Hadoop Streaming reducer: aggregate streams, watch time and buffering by content."""
import sys

current_key = None
streams = watched_seconds = buffering_events = 0

def emit(key, count, seconds, buffers):
    if key is not None:
        print(f"{key}\t{count}\t{seconds}\t{buffers}")

for line in sys.stdin:
    parts = line.rstrip("\n").split("\t")
    if len(parts) != 4:
        continue
    key = parts[0]
    try:
        count, seconds, buffers = map(int, parts[1:])
    except ValueError:
        continue
    if current_key is not None and key != current_key:
        emit(current_key, streams, watched_seconds, buffering_events)
        streams = watched_seconds = buffering_events = 0
    current_key = key
    streams += count
    watched_seconds += seconds
    buffering_events += buffers
emit(current_key, streams, watched_seconds, buffering_events)
