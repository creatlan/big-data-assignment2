# big-data-assignment2

## Implemented tasks

- Hadoop MapReduce indexer: `create_index.sh` with mapper/reducer pipelines.
- Cassandra loader: `store_index.sh`.
- Full indexing wrapper: `index.sh`.
- Spark BM25 search: `query.py` and `search.sh`.
- Bonus incremental indexing: `add_to_index.sh`.

## Run

1. Install Docker and Docker Compose.
2. Run:

```bash
docker compose up
```

This starts the Hadoop master, one Hadoop worker, and Cassandra. The master container runs `/app/app.sh`, which:

- starts Hadoop and YARN
- installs Python dependencies
- prepares `/data` and `/input/data` in HDFS
- creates the index in HDFS
- stores the index in Cassandra
- runs one search query

## Manual checks

Open a shell in the master container:

```bash
docker exec -it cluster-master bash
cd /app
```

Run the full indexing flow again:

```bash
bash index.sh
```

Run a search query:

```bash
bash search.sh "history of science"
bash search.sh "christmas story"
bash search.sh "computer network"
```

## Bonus: add one document to the index

The optional bonus task is implemented by `add_to_index.sh`. It adds one local
plain-text document to the already built Cassandra index without rebuilding all
MapReduce outputs.

Run the initial indexing flow first:

```bash
bash index.sh
```

Then add a document from the local filesystem inside the master container:

```bash
bash add_to_index.sh /app/data/12345_New_Document.txt
```

The added file must follow the assignment naming format:
`<doc_id>_<doc_title>.txt`.

For example, `12345_New_Document.txt` is parsed as:

- document id: `12345`
- document title: `New Document`

The script updates:

- Cassandra `documents`
- Cassandra `vocabulary`
- Cassandra `inverted_index`
- Cassandra `corpus_stats`
- HDFS `/data`
- HDFS `/input/data`

The script rejects duplicate document ids. If you need to replace an existing
document, rebuild the full index with `bash index.sh`.

## HDFS checks

Check the index in HDFS:

```bash
hdfs dfs -ls /indexer
hdfs dfs -cat /indexer/corpus/part-00000
hdfs dfs -cat /indexer/index/part-00000 | head
```

Check the prepared input:

```bash
hdfs dfs -ls /input/data
hdfs dfs -cat /input/data/part-00000 | head
```

Stop the project:

```bash
docker compose down -v
```
