from __future__ import annotations

import sys


VOCAB_PREFIX = "VOCAB"


last_term: str | None = None
last_document_frequency = "0"
last_total_term_frequency = "0"
term_id = 0


def flush_term(term: str | None, document_frequency: str, total_term_frequency: str, current_term_id: int) -> None:
    """Emit one vocabulary row for the current term."""

    if term is None:
        return
    print(f"{VOCAB_PREFIX}\t{term}\t{current_term_id}\t{document_frequency}\t{total_term_frequency}")

for raw_line in sys.stdin:
    line = raw_line.strip()
    if not line:
        continue

    fields = line.split("\t")
    if len(fields) < 3:
        print(f"Skipping malformed vocabulary row: {line!r}", file=sys.stderr)
        continue

    term = fields[0].strip()
    document_frequency = fields[1].strip()
    total_term_frequency = fields[2].strip()

    if not term:
        continue

    if last_term is None:
        term_id = 1
        last_term = term
        last_document_frequency = document_frequency
        last_total_term_frequency = total_term_frequency
        continue

    if term == last_term:
        last_document_frequency = document_frequency
        last_total_term_frequency = total_term_frequency
        continue

    flush_term(last_term, last_document_frequency, last_total_term_frequency, term_id)
    term_id += 1
    last_term = term
    last_document_frequency = document_frequency
    last_total_term_frequency = total_term_frequency

flush_term(last_term, last_document_frequency, last_total_term_frequency, term_id)