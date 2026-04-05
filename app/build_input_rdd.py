"""Build the normalized /input/data dataset from text documents in HDFS.

The job reads every text file from the input HDFS directory with RDD wholeTextFiles,
extracts <doc_id> and <doc_title> from the filename, filters empty documents, and
writes a single-partition text output in the format:

    <doc_id>\t<doc_title>\t<doc_text>
"""

from __future__ import annotations

import argparse
import subprocess
from pathlib import Path
from typing import Optional, Tuple

from pyspark.sql import SparkSession


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments."""

    parser = argparse.ArgumentParser(description="Build the normalized input RDD from HDFS text files.")
    parser.add_argument("--input", default="/data", help="HDFS input directory containing .txt documents")
    parser.add_argument("--output", default="/input/data", help="HDFS output directory for normalized records")
    return parser.parse_args()


def remove_hdfs_path(path: str) -> None:
    """Remove an HDFS path before writing a new output."""

    subprocess.run(["hdfs", "dfs", "-rm", "-r", "-f", path], check=False)


def split_document_name(file_name: str) -> Optional[Tuple[str, str]]:
    """Extract doc_id and doc_title from <doc_id>_<doc_title>.txt.

    Returns None when the filename does not match the required format.
    """

    if not file_name.endswith(".txt"):
        return None

    base_name = file_name[:-4]
    if "_" not in base_name:
        return None

    doc_id, doc_title = base_name.split("_", 1)
    doc_id = doc_id.strip()
    doc_title = doc_title.strip()

    if not doc_id or not doc_title:
        return None

    return doc_id, doc_title


def build_record(item: Tuple[str, str]) -> Optional[str]:
    """Convert a (path, text) pair into a normalized TSV record."""

    file_path, text = item
    file_name = Path(file_path).name
    parsed_name = split_document_name(file_name)
    if parsed_name is None:
        return None

    doc_id, doc_title = parsed_name
    doc_text = text.strip()
    if not doc_text:
        return None

    return f"{doc_id}\t{doc_title}\t{doc_text}"


def main() -> None:
    """Entry point for the RDD build job."""

    args = parse_args()
    spark = SparkSession.builder.appName("build input rdd").getOrCreate()

    try:
        remove_hdfs_path(args.output)

        source_rdd = spark.sparkContext.wholeTextFiles(args.input)
        normalized_rdd = source_rdd.flatMap(
            lambda item: [record] if (record := build_record(item)) is not None else []
        )

        # Save the result in exactly one output partition.
        normalized_rdd.coalesce(1).saveAsTextFile(args.output)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()