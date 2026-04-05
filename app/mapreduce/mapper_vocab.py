from __future__ import annotations

import sys


for raw_line in sys.stdin:
    line = raw_line.strip()
    if not line:
        continue

    fields = line.split("\t")
    if len(fields) < 5 or fields[0] != "INDEX":
        print(f"Skipping malformed line: {line!r}", file=sys.stderr)
        continue

    term = fields[1].strip()
    document_frequency = fields[2].strip()
    total_term_frequency = fields[3].strip()
    if not term:
        print(f"Skipping empty term: {line!r}", file=sys.stderr)
        continue

    print(f"{term}\t{document_frequency}\t{total_term_frequency}")