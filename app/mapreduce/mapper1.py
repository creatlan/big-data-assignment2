#!/usr/bin/env python3
import re
import sys


TOKEN_RE = re.compile(r"[a-z0-9]+")


for line in sys.stdin:
    line = line.rstrip("\n")
    parts = line.split("\t", 2)
    if len(parts) != 3:
        continue
    doc_id, title, text = parts
    doc_id = doc_id.strip()
    title = title.replace("|", " ").strip()
    tokens = TOKEN_RE.findall(text.lower())
    if not doc_id or not title or not tokens:
        continue
    print(f"DOC|{doc_id}\t{title}\t{len(tokens)}")
    for term in tokens:
        print(f"TERM|{term}|{doc_id}\t1")
