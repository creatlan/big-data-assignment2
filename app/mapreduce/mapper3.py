#!/usr/bin/env python3
import sys


for line in sys.stdin:
    line = line.rstrip("\n")
    parts = line.split("\t")
    if len(parts) != 4 or parts[0] != "DOC":
        continue
    print(f"CORPUS\t{parts[3]}")
