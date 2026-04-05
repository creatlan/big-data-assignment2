from __future__ import annotations

import re
import sys


TOKEN_PATTERN = re.compile(r"[a-z0-9']+")


def tokenize(text: str) -> list[str]:
    """Normalize text the same way as mapper1 and split it into terms."""

    return TOKEN_PATTERN.findall(text.lower())


for raw_line in sys.stdin:
    line = raw_line.strip()
    if not line:
        continue

    try:
        doc_id, doc_title, doc_text = line.split("\t", 2)
    except ValueError:
        print(f"Skipping malformed line: {line!r}", file=sys.stderr)
        continue

    doc_id = doc_id.strip()
    doc_title = doc_title.strip()
    doc_text = doc_text.strip()

    if not doc_id or not doc_title or not doc_text:
        continue

    doc_length = len(tokenize(doc_text))
    if doc_length == 0:
        continue

    print(f"{doc_id}\tDOC\t{doc_title}\t{doc_length}")
    print(f"__CORPUS__\tLEN\t{doc_length}")