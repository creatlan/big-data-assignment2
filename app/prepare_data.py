import argparse
import os
import re

from pathvalidate import sanitize_filename
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


SPACE_RE = re.compile(r"\s+")


def normalize_text(value):
    return SPACE_RE.sub(" ", value or "").strip()


def normalize_title(value):
    cleaned = sanitize_filename(str(value or "")).replace(" ", "_").strip("_")
    if not cleaned:
        cleaned = "untitled"
    return cleaned


def parse_doc(path, text):
    filename = os.path.basename(path)
    if filename.endswith(".txt"):
        filename = filename[:-4]
    if "_" not in filename:
        return None
    doc_id, raw_title = filename.split("_", 1)
    content = normalize_text(text)
    if not doc_id or not raw_title or not content:
        return None
    title = raw_title.replace("_", " ").strip()
    if not title:
        return None
    return "\t".join([doc_id, title, content])


def build_docs(args):
    spark = (
        SparkSession.builder.appName("build-documents")
        .master("local[2]")
        .config("spark.sql.parquet.enableVectorizedReader", "true")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    rows = (
        spark.read.parquet(args.parquet)
        .select("id", "title", "text")
        .where(F.col("text").isNotNull())
        .where(F.length(F.trim(F.col("text"))) > 0)
        .limit(args.count)
        .toLocalIterator()
    )
    os.makedirs(args.output_dir, exist_ok=True)
    for row in rows:
        filename = f"{row['id']}_{normalize_title(row['title'])}.txt"
        path = os.path.join(args.output_dir, filename)
        with open(path, "w", encoding="utf-8") as handle:
            handle.write(row["text"])
    spark.stop()


def build_input(args):
    spark = SparkSession.builder.appName("prepare-input").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    rdd = (
        spark.sparkContext.wholeTextFiles(args.input_dir.rstrip("/") + "/*.txt")
        .map(lambda item: parse_doc(item[0], item[1]))
        .filter(lambda item: item is not None)
        .coalesce(1)
    )
    rdd.saveAsTextFile(args.output_dir)
    spark.stop()


def main():
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="command", required=True)

    build_docs_parser = subparsers.add_parser("build-docs")
    build_docs_parser.add_argument("--parquet", required=True)
    build_docs_parser.add_argument("--output-dir", required=True)
    build_docs_parser.add_argument("--count", type=int, default=1000)

    build_input_parser = subparsers.add_parser("build-input")
    build_input_parser.add_argument("--input-dir", required=True)
    build_input_parser.add_argument("--output-dir", required=True)

    args = parser.parse_args()

    if args.command == "build-docs":
        build_docs(args)
    else:
        build_input(args)


if __name__ == "__main__":
    main()
