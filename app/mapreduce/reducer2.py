#!/usr/bin/env python3
import sys


current_term = None
postings = []


def doc_sort_key(item):
    doc_id = item[0]
    if doc_id.isdigit():
        return (0, int(doc_id))
    return (1, doc_id)


def flush(term, items):
    if term is None or not items:
        return
    ordered = sorted(items, key=doc_sort_key)
    posting_text = ",".join(f"{doc_id}:{tf}" for doc_id, tf in ordered)
    df = len(ordered)
    print(f"VOCAB\t{term}\t{df}")
    print(f"INDEX\t{term}\t{df}\t{posting_text}")


for line in sys.stdin:
    line = line.rstrip("\n")
    if not line or "\t" not in line:
        continue
    term, value = line.split("\t", 1)
    doc_id, tf = value.split(":", 1)
    if current_term is None:
        current_term = term
        postings = [(doc_id, int(tf))]
        continue
    if term == current_term:
        postings.append((doc_id, int(tf)))
        continue
    flush(current_term, postings)
    current_term = term
    postings = [(doc_id, int(tf))]


flush(current_term, postings)
