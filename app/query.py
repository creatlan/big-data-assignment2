import math
import re
import sys
import time
from collections import Counter

from cassandra.cluster import Cluster
from pyspark.sql import SparkSession


TOKEN_RE = re.compile(r"[a-z0-9]+")
K1 = 1.0
B = 0.75


def tokenize(text):
    return TOKEN_RE.findall((text or "").lower())


def connect():
    cluster = None
    last_error = None
    for _ in range(30):
        try:
            cluster = Cluster(["cassandra-server"])
            session = cluster.connect("search_engine")
            return cluster, session
        except Exception as error:
            last_error = error
            if cluster is not None:
                cluster.shutdown()
            time.sleep(5)
    raise RuntimeError(repr(last_error))


def read_query():
    if len(sys.argv) > 1:
        return " ".join(sys.argv[1:]).strip()
    return sys.stdin.read().strip()


def parse_postings(value):
    results = []
    for item in (value or "").split(","):
        if not item:
            continue
        doc_id, tf = item.split(":", 1)
        results.append((doc_id, int(tf)))
    return results


def partial_scores(row, term_counts, df_map, doc_lengths, doc_count, avgdl):
    term, postings = row
    df = df_map.get(term)
    query_tf = term_counts.get(term, 0)
    if not df or not query_tf or doc_count == 0 or avgdl == 0:
        return []
    idf = math.log(doc_count / float(df))
    results = []
    for doc_id, tf in parse_postings(postings):
        doc_length = doc_lengths.get(doc_id)
        if not doc_length:
            continue
        numerator = (K1 + 1.0) * tf
        denominator = tf + K1 * ((1.0 - B) + B * (doc_length / avgdl))
        score = query_tf * idf * numerator / denominator
        results.append((doc_id, score))
    return results


def main():
    query = read_query()
    terms = tokenize(query)
    if not terms:
        print("No query terms were provided")
        return

    cluster, session = connect()
    try:
        stats_rows = session.execute("SELECT name, value FROM corpus_stats")
        stats = {row.name: row.value for row in stats_rows}
        doc_count = int(stats.get("doc_count", 0))
        avgdl = float(stats.get("avg_doc_length", 0.0))

        term_counts = Counter(terms)
        unique_terms = sorted(term_counts.keys())

        vocab_statement = session.prepare(
            "SELECT term, df FROM vocabulary WHERE term = ?"
        )
        index_statement = session.prepare(
            "SELECT term, postings FROM inverted_index WHERE term = ?"
        )

        df_map = {}
        index_rows = []

        for term in unique_terms:
            vocab_row = session.execute(vocab_statement, [term]).one()
            index_row = session.execute(index_statement, [term]).one()
            if vocab_row is None or index_row is None:
                continue
            df_map[vocab_row.term] = vocab_row.df
            index_rows.append((index_row.term, index_row.postings))

        if not index_rows or doc_count == 0 or avgdl == 0:
            print("No matching documents found")
            return

        document_rows = session.execute("SELECT doc_id, title, length FROM documents")
        documents = [(row.doc_id, row.title, row.length) for row in document_rows]
    finally:
        session.shutdown()
        cluster.shutdown()

    spark = SparkSession.builder.appName("bm25-query").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    sc = spark.sparkContext

    doc_meta = {doc_id: title for doc_id, title, _ in documents}
    doc_lengths = {doc_id: length for doc_id, _, length in documents}

    doc_meta_bc = sc.broadcast(doc_meta)
    doc_lengths_bc = sc.broadcast(doc_lengths)
    term_counts_bc = sc.broadcast(dict(term_counts))
    df_map_bc = sc.broadcast(df_map)

    index_rdd = sc.parallelize(index_rows)
    score_rdd = (
        index_rdd.flatMap(
            lambda row: partial_scores(
                row,
                term_counts_bc.value,
                df_map_bc.value,
                doc_lengths_bc.value,
                doc_count,
                avgdl,
            )
        )
        .reduceByKey(lambda left, right: left + right)
    )

    top_results = score_rdd.takeOrdered(10, key=lambda item: -item[1])

    if not top_results:
        print("No matching documents found")
    else:
        for doc_id, _ in top_results:
            print(f"{doc_id}\t{doc_meta_bc.value.get(doc_id, '')}")

    spark.stop()


if __name__ == "__main__":
    main()
