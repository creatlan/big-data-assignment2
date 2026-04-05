from __future__ import annotations

import sys


for raw_line in sys.stdin:
    if not raw_line.strip():
        continue
    doc_id, title, doc_length = raw_line.rstrip("\n").split("\t", 2)
    print(f"STAT\t{doc_id}\t{title}\t{doc_length}")