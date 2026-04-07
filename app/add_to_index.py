#!/usr/bin/env python3
import argparse
import math
import re
import sys
import time
from collections import Counter
from pathlib import Path

TOKEN_RE = re.compile(r"[a-z0-9]+")
SPACE_RE = re.compile(r"\s+")


def tokenize(text):
    return TOKEN_RE.findall((text or "").lower())


def normalize_text(value):
    return SPACE_RE.sub(" ", value or "").strip()


def parse_document(path):
    document_path = Path(path)
    if not document_path.is_file():
        raise ValueError(f"Document file does not exist: {path}")

    if document_path.suffix.lower() != ".txt":
        raise ValueError("Document file must use the <doc_id>_<doc_title>.txt format")

    stem = document_path.stem
    if "_" not in stem:
        raise ValueError("Document file must use the <doc_id>_<doc_title>.txt format")

    doc_id, raw_title = stem.split("_", 1)
    doc_id = doc_id.strip()
    title = raw_title.replace("_", " ").strip()
    if not doc_id or not title:
        raise ValueError("Document id and title must not be empty")

    text = normalize_text(document_path.read_text(encoding="utf-8", errors="replace"))
    tokens = tokenize(text)
    if not tokens:
        raise ValueError("Document text must contain at least one indexed token")

    return doc_id, title, text, tokens


def parse_postings(value):
    postings = {}
    for item in (value or "").split(","):
        if not item:
            continue
        doc_id, tf = item.split(":", 1)
        postings[doc_id] = int(tf)
    return postings


def format_postings(postings):
    def sort_key(item):
        doc_id, _ = item
        if doc_id.isdigit():
            return (0, int(doc_id))
        return (1, doc_id)

    return ",".join(f"{doc_id}:{tf}" for doc_id, tf in sorted(postings.items(), key=sort_key))


def connect(hosts, keyspace):
    from cassandra.cluster import Cluster

    cluster = None
    last_error = None
    for _ in range(30):
        try:
            cluster = Cluster(hosts)
            session = cluster.connect(keyspace)
            return cluster, session
        except Exception as error:
            last_error = error
            if cluster is not None:
                cluster.shutdown()
            time.sleep(5)
    raise RuntimeError(repr(last_error))


def get_required_stat(stats, name):
    if name not in stats or math.isnan(float(stats[name])):
        raise RuntimeError(f"Missing corpus stat: {name}. Run index.sh first.")
    return stats[name]


def add_document(session, doc_id, title, tokens):
    existing_doc = session.execute(
        "SELECT doc_id FROM documents WHERE doc_id = %s",
        (doc_id,),
    ).one()
    if existing_doc is not None:
        raise RuntimeError(f"Document {doc_id} already exists in the index")

    stats = {
        row.name: row.value
        for row in session.execute("SELECT name, value FROM corpus_stats")
    }
    doc_count = int(get_required_stat(stats, "doc_count"))
    avg_doc_length = float(get_required_stat(stats, "avg_doc_length"))
    doc_length = len(tokens)

    term_counts = Counter(tokens)
    select_index = session.prepare(
        "SELECT term, postings FROM inverted_index WHERE term = ?"
    )
    upsert_vocab = session.prepare(
        "INSERT INTO vocabulary (term, df) VALUES (?, ?)"
    )
    upsert_index = session.prepare(
        "INSERT INTO inverted_index (term, df, postings) VALUES (?, ?, ?)"
    )

    for term, term_frequency in sorted(term_counts.items()):
        row = session.execute(select_index, (term,)).one()
        postings = parse_postings(row.postings if row is not None else "")
        if doc_id in postings:
            raise RuntimeError(f"Posting for document {doc_id} already exists for term {term}")

        postings[doc_id] = term_frequency
        document_frequency = len(postings)
        session.execute(upsert_vocab, (term, document_frequency))
        session.execute(
            upsert_index,
            (term, document_frequency, format_postings(postings)),
        )

    new_doc_count = doc_count + 1
    new_avg_doc_length = ((avg_doc_length * doc_count) + doc_length) / float(new_doc_count)

    session.execute(
        "INSERT INTO documents (doc_id, title, length) VALUES (%s, %s, %s)",
        (doc_id, title, doc_length),
    )
    session.execute(
        "INSERT INTO corpus_stats (name, value) VALUES (%s, %s)",
        ("doc_count", float(new_doc_count)),
    )
    session.execute(
        "INSERT INTO corpus_stats (name, value) VALUES (%s, %s)",
        ("avg_doc_length", new_avg_doc_length),
    )

    return doc_length, len(term_counts), new_doc_count, new_avg_doc_length


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("document")
    parser.add_argument("--host", action="append", default=["cassandra-server"])
    parser.add_argument("--keyspace", default="search_engine")
    parser.add_argument(
        "--emit-input-record",
        action="store_true",
        help="print the /input/data TSV record and do not update Cassandra",
    )
    args = parser.parse_args()

    doc_id, title, text, tokens = parse_document(args.document)

    if args.emit_input_record:
        print("\t".join([doc_id, title, text]))
        return

    cluster, session = connect(args.host, args.keyspace)
    try:
        doc_length, unique_terms, new_doc_count, new_avg_doc_length = add_document(
            session, doc_id, title, tokens
        )
        print(f"Added document: {doc_id}\t{title}")
        print(f"Document length: {doc_length}")
        print(f"Updated unique terms: {unique_terms}")
        print(f"New corpus document count: {new_doc_count}")
        print(f"New average document length: {new_avg_doc_length:.6f}")
    finally:
        session.shutdown()
        cluster.shutdown()


if __name__ == "__main__":
    try:
        main()
    except Exception as error:
        print(f"add_to_index failed: {error}", file=sys.stderr)
        sys.exit(1)
