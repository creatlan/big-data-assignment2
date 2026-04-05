from __future__ import annotations

import sys


CORPUS_KEY = "__CORPUS__"


def emit_document_row(doc_id: str, doc_title: str, doc_length: int) -> None:
    """Write one validated document statistics row to stdout."""

    print(f"STAT\t{doc_id}\t{doc_title}\t{doc_length}")


def emit_corpus_row(document_count: int, total_length: int) -> None:
    """Write corpus-level BM25 statistics row to stdout."""

    average_document_length = (total_length / document_count) if document_count else 0.0
    print(f"CORPUS\t{document_count}\t{average_document_length:.10f}\t{total_length}")


current_doc_id: str | None = None
current_doc_title: str | None = None
current_doc_length: int | None = None

document_count = 0
total_length = 0


def flush_current_document() -> None:
    global current_doc_id, current_doc_title, current_doc_length
    if current_doc_id is None or current_doc_title is None or current_doc_length is None:
        return
    emit_document_row(current_doc_id, current_doc_title, current_doc_length)
    current_doc_id = None
    current_doc_title = None
    current_doc_length = None

for raw_line in sys.stdin:
    line = raw_line.strip()
    if not line:
        continue

    try:
        key, record_type, payload = line.split("\t", 2)
    except ValueError:
        print(f"Skipping malformed line: {line!r}", file=sys.stderr)
        continue

    key = key.strip()
    record_type = record_type.strip()
    payload = payload.strip()

    if not key or not record_type or not payload:
        print(f"Skipping incomplete row: {line!r}", file=sys.stderr)
        continue

    if key == CORPUS_KEY and record_type == "LEN":
        try:
            doc_length = int(payload)
        except ValueError:
            print(f"Skipping corpus row with invalid length: {line!r}", file=sys.stderr)
            continue
        if doc_length < 0:
            print(f"Skipping corpus row with negative length: {line!r}", file=sys.stderr)
            continue

        document_count += 1
        total_length += doc_length
        continue

    if record_type != "DOC":
        print(f"Skipping unknown record type: {line!r}", file=sys.stderr)
        continue

    fields = payload.split("\t", 1)
    if len(fields) != 2:
        fields = payload.split("|", 1)
    if len(fields) != 2:
        print(f"Skipping malformed DOC payload: {line!r}", file=sys.stderr)
        continue

    doc_title = fields[0].strip()
    doc_length_raw = fields[1].strip()

    try:
        doc_length = int(doc_length_raw)
    except ValueError:
        print(f"Skipping document row with invalid length: {line!r}", file=sys.stderr)
        continue

    if doc_length < 0:
        print(f"Skipping document row with negative length: {line!r}", file=sys.stderr)
        continue

    if current_doc_id is None:
        current_doc_id = key
    if key != current_doc_id:
        flush_current_document()
        current_doc_id = key

    current_doc_title = doc_title
    current_doc_length = doc_length

flush_current_document()
emit_corpus_row(document_count, total_length)
