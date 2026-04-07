#!/usr/bin/env python3
import sys


doc_count = 0
total_length = 0


for line in sys.stdin:
    line = line.rstrip("\n")
    if not line or "\t" not in line:
        continue
    _, value = line.split("\t", 1)
    doc_count += 1
    total_length += int(value)


if doc_count > 0:
    average = total_length / float(doc_count)
    print(f"CORPUS\t{doc_count}\t{average}")
