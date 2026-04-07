#!/usr/bin/env python3
import sys


current_key = None
current_values = []


def flush(key, values):
    if key is None or not values:
        return
    parts = key.split("|")
    if parts[0] == "DOC" and len(parts) == 2:
        title = ""
        length = 0
        for value in values:
            value_parts = value.split("\t")
            if len(value_parts) != 2:
                continue
            title = value_parts[0]
            length = int(value_parts[1])
        if title and length > 0:
            print(f"DOC\t{parts[1]}\t{title}\t{length}")
    elif parts[0] == "TERM" and len(parts) == 3:
        total = 0
        for value in values:
            if value:
                total += int(value)
        if total > 0:
            print(f"TERM\t{parts[1]}\t{parts[2]}\t{total}")


for line in sys.stdin:
    line = line.rstrip("\n")
    if not line:
        continue
    if "\t" not in line:
        continue
    key, value = line.split("\t", 1)
    if current_key is None:
        current_key = key
        current_values = [value]
        continue
    if key == current_key:
        current_values.append(value)
        continue
    flush(current_key, current_values)
    current_key = key
    current_values = [value]


flush(current_key, current_values)
