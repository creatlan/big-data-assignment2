from __future__ import annotations

import argparse
import os
import re
import sys

from pyspark.sql import DataFrame, SparkSession, functions as F


# Keep tokenization compatible with indexer mapper logic.
TOKEN_PATTERN = re.compile(r"[a-z0-9']+")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="PySpark BM25 search over Cassandra/ScyllaDB")
    parser.add_argument("--host", default="cassandra-server", help="Cassandra/ScyllaDB host")
    parser.add_argument("--port", type=int, default=9042, help="Cassandra/ScyllaDB native port")
    parser.add_argument("--keyspace", default="search_index", help="Keyspace name")
    parser.add_argument("--top_k", type=int, default=10, help="Number of top results to output")
    return parser.parse_args()


def normalize_query(text: str) -> list[str]:
    return [token for token in TOKEN_PATTERN.findall(text.lower()) if token]


def read_query_from_stdin() -> str:
    stdin_query = sys.stdin.read().strip()
    if stdin_query:
        return stdin_query

    # In YARN cluster deploy-mode, stdin from submit host may not reach the driver.
    return os.environ.get("QUERY_TEXT", "").strip()


def build_spark_session(host: str, port: int) -> SparkSession:
    return (
        SparkSession.builder.appName("bm25-query")
        .config("spark.cassandra.connection.host", host)
        .config("spark.cassandra.connection.port", str(port))
        .getOrCreate()
    )


def read_cassandra_table(spark: SparkSession, keyspace: str, table: str) -> DataFrame:
    return (
        spark.read.format("org.apache.spark.sql.cassandra")
        .option("keyspace", keyspace)
        .option("table", table)
        .load()
    )


def get_collection_stats(spark: SparkSession, keyspace: str) -> tuple[float, float]:
    # Preferred schema from assignment statement.
    try:
        collection_stats_df = read_cassandra_table(spark, keyspace, "collection_stats")
        stats = (
            collection_stats_df.where(F.col("stat_name").isin("total_docs", "avg_doc_length"))
            .select("stat_name", "stat_value")
            .collect()
        )
        stat_map: dict[str, float] = {
            str(row["stat_name"]): float(row["stat_value"]) for row in stats if row["stat_name"] is not None
        }
        total_docs = stat_map.get("total_docs", 0.0)
        avg_doc_length = stat_map.get("avg_doc_length", 0.0)
        if total_docs > 0 and avg_doc_length > 0:
            return total_docs, avg_doc_length
    except Exception:
        pass

    # Backward-compatible schema used by current store_index.py.
    try:
        corpus_stats_df = read_cassandra_table(spark, keyspace, "corpus_stats")
        row = (
            corpus_stats_df.where(F.col("stat_key") == F.lit("bm25"))
            .select("document_count", "average_document_length")
            .limit(1)
            .collect()
        )
        if row:
            return float(row[0]["document_count"] or 0.0), float(row[0]["average_document_length"] or 0.0)
    except Exception:
        pass

    return 0.0, 0.0


def main() -> None:
    args = parse_args()
    raw_query = read_query_from_stdin()
    query_terms = sorted(set(normalize_query(raw_query)))

    if not query_terms:
        print("Empty query after normalization. Please enter at least one valid term.")
        return

    top_k = max(1, args.top_k)
    k1 = 1.5
    b = 0.75

    spark = build_spark_session(args.host, args.port)

    try:
        vocabulary_df = read_cassandra_table(spark, args.keyspace, "vocabulary").select("term")
        inverted_index_raw_df = read_cassandra_table(spark, args.keyspace, "inverted_index")
        inverted_index_df = inverted_index_raw_df.select(
            "term",
            "doc_id",
            F.coalesce(F.col("tf"), F.col("term_frequency")).alias("tf"),
            F.coalesce(F.col("df"), F.col("document_frequency")).alias("df"),
        )
        document_stats_df = read_cassandra_table(spark, args.keyspace, "document_stats").select(
            "doc_id", "title", "doc_length"
        )

        query_terms_df = spark.createDataFrame([(term,) for term in query_terms], ["term"])
        valid_terms_df = query_terms_df.join(vocabulary_df, on="term", how="inner").distinct()

        if valid_terms_df.rdd.isEmpty():
            print("No results found for the given query.")
            return

        total_docs, avg_doc_length = get_collection_stats(spark, args.keyspace)
        if total_docs <= 0 or avg_doc_length <= 0:
            print("No results found: collection statistics are missing or invalid.")
            return

        n_docs_lit = F.lit(float(total_docs))
        avgdl_lit = F.lit(float(avg_doc_length))
        k1_lit = F.lit(float(k1))
        b_lit = F.lit(float(b))

        # BM25 formula:
        # idf = log(1 + (N - df + 0.5) / (df + 0.5))
        # score = idf * ((tf * (k1 + 1)) / (tf + k1 * (1 - b + b * dl / avgdl)))
        ranked_df = (
            inverted_index_df.join(valid_terms_df, on="term", how="inner")
            .join(document_stats_df, on="doc_id", how="inner")
            .where((F.col("tf") > 0) & (F.col("df") > 0) & (F.col("doc_length") > 0))
            .withColumn(
                "idf",
                F.log(F.lit(1.0) + ((n_docs_lit - F.col("df") + F.lit(0.5)) / (F.col("df") + F.lit(0.5)))),
            )
            .withColumn(
                "bm25_part",
                F.col("idf")
                * (
                    (F.col("tf") * (k1_lit + F.lit(1.0)))
                    / (
                        F.col("tf")
                        + k1_lit
                        * (F.lit(1.0) - b_lit + b_lit * (F.col("doc_length") / avgdl_lit))
                    )
                ),
            )
            .groupBy("doc_id")
            .agg(F.first("title").alias("title"), F.sum("bm25_part").alias("score"))
            .orderBy(F.desc("score"), F.asc("doc_id"))
            .limit(top_k)
        )

        results = ranked_df.select("doc_id", "title").collect()
        if not results:
            print("No results found for the given query.")
            return

        for row in results:
            print(f"{row['doc_id']}\t{row['title']}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
