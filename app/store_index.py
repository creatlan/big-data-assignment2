from __future__ import annotations

import argparse
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Iterator, Sequence

from cassandra.cluster import Cluster
from cassandra.concurrent import execute_concurrent_with_args


KEYSPACE_NAME_PATTERN = re.compile(r"^[A-Za-z][A-Za-z0-9_]*$")
PART_FILE_PATTERN = re.compile(r"^part-.*$")


@dataclass(frozen=True)
class VocabularyRow:
    term: str
    term_id: int
    document_frequency: int
    total_term_frequency: int


@dataclass(frozen=True)
class IndexPostingRow:
    term: str
    doc_id: str
    tf: int
    df: int


@dataclass(frozen=True)
class DocumentStatRow:
    doc_id: str
    title: str
    doc_length: int


@dataclass(frozen=True)
class CollectionStatsRow:
    total_docs: int
    avg_doc_length: float


def iter_input_files(path: str) -> Iterator[Path]:
    input_path = Path(path)
    if input_path.is_dir():
        for child_path in sorted(input_path.iterdir()):
            if child_path.is_file() and PART_FILE_PATTERN.match(child_path.name):
                yield child_path
    else:
        yield input_path


def read_tsv_lines(path: str) -> Iterator[list[str]]:
    for file_path in iter_input_files(path):
        for raw_line in file_path.read_text(encoding="utf-8").splitlines():
            line = raw_line.strip()
            if line:
                yield line.split("\t")


def validate_keyspace_name(keyspace: str) -> str:
    keyspace = keyspace.strip()
    if not KEYSPACE_NAME_PATTERN.match(keyspace):
        raise ValueError(
            "keyspace must start with a letter and contain only letters, digits, or underscores"
        )
    return keyspace


def parse_vocabulary(path: str) -> list[VocabularyRow]:
    rows: list[VocabularyRow] = []
    for columns in read_tsv_lines(path):
        if len(columns) < 5 or columns[0] != "VOCAB":
            continue
        rows.append(
            VocabularyRow(
                term=columns[1].strip(),
                term_id=int(columns[2]),
                document_frequency=int(columns[3]),
                total_term_frequency=int(columns[4]),
            )
        )
    return rows


def parse_index(path: str) -> list[IndexPostingRow]:
    rows: list[IndexPostingRow] = []
    for columns in read_tsv_lines(path):
        if len(columns) < 5 or columns[0] != "INDEX":
            continue

        term = columns[1].strip()
        document_frequency = int(columns[2])
        postings_field = columns[4]

        for posting in postings_field.split(";"):
            posting = posting.strip()
            if not posting:
                continue

            posting_fields = posting.split("|")
            if len(posting_fields) != 4:
                continue

            rows.append(
                IndexPostingRow(
                    term=term,
                    doc_id=posting_fields[0].strip(),
                    tf=int(posting_fields[2]),
                    df=document_frequency,
                )
            )

    return rows


def parse_doc_stats(path: str) -> tuple[list[DocumentStatRow], CollectionStatsRow | None]:
    rows: list[DocumentStatRow] = []
    collection_stats: CollectionStatsRow | None = None

    for columns in read_tsv_lines(path):
        if not columns:
            continue

        if columns[0] == "STAT" and len(columns) >= 4:
            rows.append(
                DocumentStatRow(
                    doc_id=columns[1].strip(),
                    title=columns[2].strip(),
                    doc_length=int(columns[3]),
                )
            )
            continue

        if columns[0] == "CORPUS" and len(columns) >= 4:
            collection_stats = CollectionStatsRow(
                total_docs=int(columns[1]),
                avg_doc_length=float(columns[2]),
            )

    return rows, collection_stats


def ensure_schema(session, keyspace: str) -> None:
    session.execute(
        f"""
        CREATE KEYSPACE IF NOT EXISTS {keyspace}
        WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': 1}}
        """
    )
    session.set_keyspace(keyspace)

    session.execute(
        """
        CREATE TABLE IF NOT EXISTS vocabulary (
            term text PRIMARY KEY,
            term_id int,
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
            tf int,
            df int,
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
        CREATE TABLE IF NOT EXISTS collection_stats (
            stat_name text PRIMARY KEY,
            stat_value double
        )
        """
    )


def load_vocabulary(session, rows: Sequence[VocabularyRow]) -> None:
    statement = session.prepare(
        "INSERT INTO vocabulary (term, term_id, document_frequency, total_term_frequency) VALUES (?, ?, ?, ?)"
    )
    execute_concurrent_with_args(
        session,
        statement,
        (
            (row.term, row.term_id, row.document_frequency, row.total_term_frequency)
            for row in rows
        ),
        concurrency=32,
    )


def load_index(session, rows: Sequence[IndexPostingRow]) -> None:
    statement = session.prepare(
        "INSERT INTO inverted_index (term, doc_id, tf, df) VALUES (?, ?, ?, ?)"
    )
    execute_concurrent_with_args(
        session,
        statement,
        (
            (
                row.term,
                row.doc_id,
                row.tf,
                row.df,
            )
            for row in rows
        ),
        concurrency=32,
    )


def load_doc_stats(session, rows: Sequence[DocumentStatRow]) -> None:
    statement = session.prepare(
        "INSERT INTO document_stats (doc_id, title, doc_length) VALUES (?, ?, ?)"
    )
    execute_concurrent_with_args(
        session,
        statement,
        ((row.doc_id, row.title, row.doc_length) for row in rows),
        concurrency=32,
    )


def load_collection_stats(session, row: CollectionStatsRow) -> None:
    session.execute(
        "INSERT INTO collection_stats (stat_name, stat_value) VALUES (?, ?)",
        ("total_docs", float(row.total_docs)),
    )
    session.execute(
        "INSERT INTO collection_stats (stat_name, stat_value) VALUES (?, ?)",
        ("avg_doc_length", float(row.avg_doc_length)),
    )


def derive_collection_stats(rows: Sequence[DocumentStatRow]) -> CollectionStatsRow:
    total_docs = len(rows)
    total_doc_length = sum(row.doc_length for row in rows)
    avg_doc_length = (total_doc_length / total_docs) if total_docs else 0.0
    return CollectionStatsRow(
        total_docs=total_docs,
        avg_doc_length=avg_doc_length,
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Load index data into Cassandra/ScyllaDB.")
    parser.add_argument("--host", default="cassandra-server", help="Cassandra host")
    parser.add_argument("--port", type=int, default=9042, help="Cassandra native port")
    parser.add_argument("--keyspace", default="search_index", help="Target keyspace")
    parser.add_argument("--index_file", required=True, help="Local merged index TSV")
    parser.add_argument("--doc_stats_file", required=True, help="Local merged document stats TSV")
    parser.add_argument("--vocabulary_file", required=True, help="Local merged vocabulary TSV")
    parser.add_argument("--incremental", action="store_true", help="Reserved optional mode")
    args = parser.parse_args()

    keyspace = validate_keyspace_name(args.keyspace)

    vocabulary_rows = parse_vocabulary(args.vocabulary_file)
    index_rows = parse_index(args.index_file)
    document_rows, collection_stats = parse_doc_stats(args.doc_stats_file)

    if collection_stats is None:
        collection_stats = derive_collection_stats(document_rows)

    cluster = Cluster([args.host], port=args.port)
    session = cluster.connect()
    try:
        ensure_schema(session, keyspace)
        load_vocabulary(session, vocabulary_rows)
        load_index(session, index_rows)
        load_doc_stats(session, document_rows)
        load_collection_stats(session, collection_stats)
        print(
            "Loaded "
            f"{len(vocabulary_rows)} vocabulary rows, "
            f"{len(index_rows)} postings, "
            f"{len(document_rows)} document stats rows"
        )
    finally:
        session.shutdown()
        cluster.shutdown()


if __name__ == "__main__":
    main()
