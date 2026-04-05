from __future__ import annotations

import argparse
import math
import re
import sys
from dataclasses import dataclass

from cassandra.cluster import Cluster
from pyspark.sql import SparkSession


TOKEN_PATTERN = re.compile(r"[a-z0-9']+")


@dataclass(frozen=True)
class QueryTermStats:
    term: str
    document_frequency: int


@dataclass(frozen=True)
class PostingRow:
    term: str
    doc_id: str
    title: str
    term_frequency: int
    doc_length: int
    document_frequency: int


def tokenize(text: str) -> list[str]:
    return TOKEN_PATTERN.findall(text.lower())


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="BM25 search over Cassandra-backed index")
    parser.add_argument("query", nargs="*", help="Query text. If empty, stdin will be used.")
    parser.add_argument("--host", default="cassandra-server", help="Cassandra host")
    parser.add_argument("--port", type=int, default=9042, help="Cassandra port")
    parser.add_argument("--keyspace", default="search_index", help="Cassandra keyspace")
    parser.add_argument("--k1", type=float, default=1.0, help="BM25 k1 parameter")
    parser.add_argument("--b", type=float, default=0.75, help="BM25 b parameter")
    return parser.parse_args()


def read_query(args: argparse.Namespace) -> str:
    if args.query:
        return " ".join(args.query).strip()
    return sys.stdin.read().strip()


def fetch_corpus_stats(session) -> tuple[int, float]:
    row = session.execute(
        "SELECT document_count, average_document_length FROM corpus_stats WHERE stat_key = %s",
        ("bm25",),
    ).one()

    if row is not None and row.document_count and row.average_document_length:
        return int(row.document_count), float(row.average_document_length)

    rows = list(session.execute("SELECT doc_length FROM document_stats"))
    document_count = len(rows)
    average_document_length = (sum(r.doc_length for r in rows) / document_count) if document_count else 0.0
    return document_count, average_document_length


def fetch_query_terms(session, terms: list[str]) -> list[QueryTermStats]:
    statement = session.prepare("SELECT term, document_frequency FROM vocabulary WHERE term = ?")
    rows: list[QueryTermStats] = []
    for term in terms:
        record = session.execute(statement, (term,)).one()
        if record is None or record.document_frequency is None:
            continue
        rows.append(QueryTermStats(term=record.term, document_frequency=int(record.document_frequency)))
    return rows


def fetch_postings(session, terms: list[str]) -> list[PostingRow]:
    statement = session.prepare(
        "SELECT term, doc_id, title, term_frequency, doc_length, document_frequency FROM inverted_index WHERE term = ?"
    )
    rows: list[PostingRow] = []
    for term in terms:
        for record in session.execute(statement, (term,)):
            rows.append(
                PostingRow(
                    term=record.term,
                    doc_id=record.doc_id,
                    title=record.title or "untitled",
                    term_frequency=int(record.term_frequency or 0),
                    doc_length=int(record.doc_length or 0),
                    document_frequency=int(record.document_frequency or 0),
                )
            )
    return rows


def bm25_score(tf: int, df: int, dl: int, n_docs: int, avgdl: float, k1: float, b: float) -> float:
    if tf <= 0 or df <= 0 or dl <= 0 or n_docs <= 0 or avgdl <= 0:
        return 0.0

    idf = math.log(n_docs / df)
    denominator = k1 * ((1.0 - b) + b * (dl / avgdl)) + tf
    if denominator == 0:
        return 0.0

    return idf * ((k1 + 1.0) * tf) / denominator


def main() -> None:
    args = parse_args()
    query_text = read_query(args)
    query_terms = sorted(set(tokenize(query_text)))

    if not query_terms:
        print("Empty query")
        return

    spark = SparkSession.builder.appName("search query bm25").getOrCreate()
    sc = spark.sparkContext

    cluster = Cluster([args.host], port=args.port)
    session = cluster.connect(args.keyspace)

    try:
        n_docs, avgdl = fetch_corpus_stats(session)
        term_rows = fetch_query_terms(session, query_terms)
        if not term_rows or n_docs <= 0 or avgdl <= 0:
            print("No results")
            return

        selected_terms = [row.term for row in term_rows]
        postings = fetch_postings(session, selected_terms)
        if not postings:
            print("No results")
            return

        n_docs_bc = sc.broadcast(n_docs)
        avgdl_bc = sc.broadcast(avgdl)
        k1_bc = sc.broadcast(args.k1)
        b_bc = sc.broadcast(args.b)

        ranked = (
            sc.parallelize(postings)
            .map(
                lambda row: (
                    (row.doc_id, row.title),
                    bm25_score(
                        row.term_frequency,
                        row.document_frequency,
                        row.doc_length,
                        n_docs_bc.value,
                        avgdl_bc.value,
                        k1_bc.value,
                        b_bc.value,
                    ),
                )
            )
            .reduceByKey(lambda a, b: a + b)
            .takeOrdered(10, key=lambda item: -item[1])
        )

        if not ranked:
            print("No results")
            return

        for (doc_id, title), score in ranked:
            print(f"{doc_id}\t{title}\t{score:.6f}")
    finally:
        session.shutdown()
        cluster.shutdown()
        spark.stop()


if __name__ == "__main__":
    main()
