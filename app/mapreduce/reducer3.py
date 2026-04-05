from __future__ import annotations

import sys


for raw_line in sys.stdin:
    if not raw_line.strip():
        continue
    term, document_frequency, total_term_frequency = raw_line.rstrip("\n").split("\t", 2)
    print(f"VOCAB\t{term}\t{document_frequency}\t{total_term_frequency}")