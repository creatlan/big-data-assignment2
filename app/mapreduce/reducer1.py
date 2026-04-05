from __future__ import annotations

import sys

def flush_term(term: str | None, postings: list[str], total_term_frequency: int) -> None:
    """Emit one inverted-index row for the accumulated term."""

    if term is None:
        return
    document_frequency = len(postings)
    print(f"INDEX\t{term}\t{document_frequency}\t{total_term_frequency}\t{';'.join(postings)}")


current_term: str | None = None
current_postings: list[str] = []
current_total_term_frequency = 0

for raw_line in sys.stdin:
    line = raw_line.strip()
    if not line:
        continue

    try:
        term, payload = line.split("\t", 1)
    except ValueError:
        print(f"Skipping malformed line: {line!r}", file=sys.stderr)
        continue

    term = term.strip()
    payload = payload.strip()

    if not term or not payload:
        continue

    fields = payload.split("|")
    if len(fields) != 4:
        print(f"Skipping malformed payload: {line!r}", file=sys.stderr)
        continue

    try:
        term_frequency = int(fields[2])
    except ValueError:
        print(f"Skipping payload with invalid frequency: {line!r}", file=sys.stderr)
        continue

    if current_term is None:
        current_term = term

    if term != current_term:
        flush_term(current_term, current_postings, current_total_term_frequency)
        current_term = term
        current_postings = []
        current_total_term_frequency = 0

    current_postings.append(payload)
    current_total_term_frequency += term_frequency

flush_term(current_term, current_postings, current_total_term_frequency)