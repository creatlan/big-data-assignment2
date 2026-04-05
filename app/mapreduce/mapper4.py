from __future__ import annotations

import sys


for raw_line in sys.stdin:
    if not raw_line.strip():
        continue
    fields = raw_line.rstrip("\n").split("\t")
    if fields[0] == "DOC":
        doc_id = fields[1]
        title = fields[2]
        doc_length = fields[3]
        print(f"{doc_id}\t{title}\t{doc_length}")