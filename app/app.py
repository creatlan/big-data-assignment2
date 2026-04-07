import argparse
import subprocess
import time

from cassandra.cluster import Cluster
from cassandra.concurrent import execute_concurrent_with_args


def read_hdfs_lines(path):
    process = subprocess.Popen(
        ["hdfs", "dfs", "-cat", f"{path}/part-*"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        for line in process.stdout:
            line = line.rstrip("\n")
            if line:
                yield line
    finally:
        stderr = process.stderr.read()
        code = process.wait()
        if code != 0:
            raise RuntimeError(stderr.strip() or f"Failed to read {path}")


def connect(hosts, keyspace):
    cluster = None
    last_error = None
    for _ in range(30):
        try:
            cluster = Cluster(hosts)
            session = cluster.connect()
            session.execute(
                f"CREATE KEYSPACE IF NOT EXISTS {keyspace} "
                "WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}"
            )
            session.set_keyspace(keyspace)
            return cluster, session
        except Exception as error:
            last_error = error
            if cluster is not None:
                cluster.shutdown()
            time.sleep(5)
    raise RuntimeError(repr(last_error))


def flush_rows(session, statement, rows):
    if rows:
        execute_concurrent_with_args(session, statement, rows, concurrency=100)
        rows.clear()


def create_tables(session):
    session.execute("DROP TABLE IF EXISTS documents")
    session.execute("DROP TABLE IF EXISTS vocabulary")
    session.execute("DROP TABLE IF EXISTS inverted_index")
    session.execute("DROP TABLE IF EXISTS corpus_stats")

    session.execute(
        "CREATE TABLE documents (doc_id text PRIMARY KEY, title text, length int)"
    )
    session.execute("CREATE TABLE vocabulary (term text PRIMARY KEY, df int)")
    session.execute(
        "CREATE TABLE inverted_index (term text PRIMARY KEY, df int, postings text)"
    )
    session.execute("CREATE TABLE corpus_stats (name text PRIMARY KEY, value double)")


def load_documents(session, path):
    statement = session.prepare(
        "INSERT INTO documents (doc_id, title, length) VALUES (?, ?, ?)"
    )
    rows = []
    count = 0
    for line in read_hdfs_lines(path):
        parts = line.split("\t")
        if len(parts) != 4 or parts[0] != "DOC":
            continue
        rows.append((parts[1], parts[2], int(parts[3])))
        count += 1
        if len(rows) >= 500:
            flush_rows(session, statement, rows)
    flush_rows(session, statement, rows)
    return count


def load_index(session, path):
    vocab_statement = session.prepare(
        "INSERT INTO vocabulary (term, df) VALUES (?, ?)"
    )
    index_statement = session.prepare(
        "INSERT INTO inverted_index (term, df, postings) VALUES (?, ?, ?)"
    )
    vocab_rows = []
    index_rows = []
    vocab_count = 0
    index_count = 0
    for line in read_hdfs_lines(path):
        parts = line.split("\t", 3)
        if len(parts) < 3:
            continue
        if parts[0] == "VOCAB":
            vocab_rows.append((parts[1], int(parts[2])))
            vocab_count += 1
            if len(vocab_rows) >= 500:
                flush_rows(session, vocab_statement, vocab_rows)
        elif parts[0] == "INDEX" and len(parts) == 4:
            index_rows.append((parts[1], int(parts[2]), parts[3]))
            index_count += 1
            if len(index_rows) >= 500:
                flush_rows(session, index_statement, index_rows)
    flush_rows(session, vocab_statement, vocab_rows)
    flush_rows(session, index_statement, index_rows)
    return vocab_count, index_count


def load_corpus(session, path):
    statement = session.prepare(
        "INSERT INTO corpus_stats (name, value) VALUES (?, ?)"
    )
    rows = []
    for line in read_hdfs_lines(path):
        parts = line.split("\t")
        if len(parts) != 3 or parts[0] != "CORPUS":
            continue
        rows.append(("doc_count", float(parts[1])))
        rows.append(("avg_doc_length", float(parts[2])))
    flush_rows(session, statement, rows)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--stats-path", default="/indexer/stats")
    parser.add_argument("--index-path", default="/indexer/index")
    parser.add_argument("--corpus-path", default="/indexer/corpus")
    parser.add_argument("--host", action="append", default=["cassandra-server"])
    parser.add_argument("--keyspace", default="search_engine")
    args = parser.parse_args()

    cluster, session = connect(args.host, args.keyspace)
    try:
        create_tables(session)
        document_count = load_documents(session, args.stats_path)
        vocabulary_count, index_count = load_index(session, args.index_path)
        load_corpus(session, args.corpus_path)
        print(f"Loaded documents: {document_count}")
        print(f"Loaded vocabulary terms: {vocabulary_count}")
        print(f"Loaded index rows: {index_count}")
    finally:
        session.shutdown()
        cluster.shutdown()


if __name__ == "__main__":
    main()
