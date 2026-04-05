from __future__ import annotations

import sys


for raw_line in sys.stdin:
    if not raw_line.strip():
        continue
    fields = raw_line.rstrip("\n").split("\t")
    if fields[0] == "INDEX":
        term = fields[1]
        document_frequency = fields[2]
        total_term_frequency = fields[3]
        print(f"{term}\t{document_frequency}\t{total_term_frequency}")