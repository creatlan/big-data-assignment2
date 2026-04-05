from __future__ import annotations

import argparse
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

from cassandra.cluster import Cluster
from cassandra.concurrent import execute_concurrent_with_args


@dataclass(frozen=True)
class VocabularyRow:
    term: str
    document_frequency: int
    total_term_frequency: int


@dataclass(frozen=True)
class IndexRow:
    term: str
    doc_id: str
    title: str
    term_frequency: int
    doc_length: int


@dataclass(frozen=True)
class DocumentStatRow:
    doc_id: str
    title: str
    doc_length: int


def read_tsv_lines(path: str) -> Iterable[list[str]]:
    for raw_line in Path(path).read_text(encoding="utf-8").splitlines():
        if raw_line.strip():
            yield raw_line.split("\t")


def parse_vocab(path: str) -> list[VocabularyRow]:
    rows: list[VocabularyRow] = []
    for columns in read_tsv_lines(path):
        if len(columns) >= 4 and columns[0] == "VOCAB":
            rows.append(VocabularyRow(columns[1], int(columns[2]), int(columns[3])))
    return rows


def parse_index(path: str) -> list[IndexRow]:
    rows: list[IndexRow] = []
    for columns in read_tsv_lines(path):
        if len(columns) < 5 or columns[0] != "INDEX":
            continue
        term = columns[1]
        postings = columns[4]
        for posting in postings.split(";"):
            if not posting:
                continue
            doc_id, title, term_frequency, doc_length = posting.split("|", 3)
            rows.append(IndexRow(term, doc_id, title, int(term_frequency), int(doc_length)))
    return rows


def parse_stats(path: str) -> list[DocumentStatRow]:
    rows: list[DocumentStatRow] = []
    for columns in read_tsv_lines(path):
        if len(columns) >= 4 and columns[0] == "STAT":
            rows.append(DocumentStatRow(columns[1], columns[2], int(columns[3])))
    return rows


def ensure_keyspace_and_tables(session) -> None:
    session.execute(
        """
        CREATE KEYSPACE IF NOT EXISTS search_index
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}
        """
    )
    session.set_keyspace("search_index")
    session.execute(
        """
        CREATE TABLE IF NOT EXISTS vocabulary (
            term text PRIMARY KEY,
            document_frequency int,
            total_term_frequency int
        )
        """
    )
    session.execute(
        """
        CREATE TABLE IF NOT EXISTS inverted_index (
            term text,
            doc_id text,
            title text,
            term_frequency int,
            doc_length int,
            PRIMARY KEY ((term), doc_id)
        )
        """
    )
    session.execute(
        """
        CREATE TABLE IF NOT EXISTS document_stats (
            doc_id text PRIMARY KEY,
            title text,
            doc_length int
        )
        """
    )
    session.execute(
        """
        CREATE TABLE IF NOT EXISTS corpus_stats (
            stat_key text PRIMARY KEY,
            document_count int,
            average_document_length double
        )
        """
    )


def load_rows(session, vocabulary_rows: list[VocabularyRow], index_rows: list[IndexRow], document_rows: list[DocumentStatRow]) -> None:
    vocab_stmt = session.prepare("INSERT INTO vocabulary (term, document_frequency, total_term_frequency) VALUES (?, ?, ?)")
    index_stmt = session.prepare(
        "INSERT INTO inverted_index (term, doc_id, title, term_frequency, doc_length) VALUES (?, ?, ?, ?, ?)"
    )
    doc_stmt = session.prepare("INSERT INTO document_stats (doc_id, title, doc_length) VALUES (?, ?, ?)")
    corpus_stmt = session.prepare(
        "INSERT INTO corpus_stats (stat_key, document_count, average_document_length) VALUES (?, ?, ?)"
    )

    execute_concurrent_with_args(session, vocab_stmt, ((row.term, row.document_frequency, row.total_term_frequency) for row in vocabulary_rows), concurrency=32)
    execute_concurrent_with_args(
        session,
        index_stmt,
        ((row.term, row.doc_id, row.title, row.term_frequency, row.doc_length) for row in index_rows),
        concurrency=32,
    )
    execute_concurrent_with_args(session, doc_stmt, ((row.doc_id, row.title, row.doc_length) for row in document_rows), concurrency=32)

    document_count = len(document_rows)
    average_document_length = sum(row.doc_length for row in document_rows) / document_count if document_count else 0.0
    session.execute(corpus_stmt, ("bm25", document_count, average_document_length))


def main() -> None:
    parser = argparse.ArgumentParser(description="Load index data into Cassandra/ScyllaDB.")
    parser.add_argument("--host", default="cassandra-server", help="Cassandra host")
    parser.add_argument("--port", type=int, default=9042, help="Cassandra native transport port")
    parser.add_argument("--vocab", required=True, help="Merged vocabulary TSV from HDFS")
    parser.add_argument("--index", required=True, help="Merged inverted-index TSV from HDFS")
    parser.add_argument("--stats", required=True, help="Merged document-stats TSV from HDFS")
    args = parser.parse_args()

    cluster = Cluster([args.host], port=args.port)
    session = cluster.connect()
    try:
        ensure_keyspace_and_tables(session)
        vocabulary_rows = parse_vocab(args.vocab)
        index_rows = parse_index(args.index)
        document_rows = parse_stats(args.stats)
        load_rows(session, vocabulary_rows, index_rows, document_rows)
        print(
            f"Loaded {len(vocabulary_rows)} vocabulary rows, {len(index_rows)} postings, and {len(document_rows)} document stats rows"
        )
    finally:
        session.shutdown()
        cluster.shutdown()


if __name__ == "__main__":
    main()