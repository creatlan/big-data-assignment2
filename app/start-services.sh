#!/bin/bash
set -e

if [ -n "$HADOOP_HOME" ]; then
  printf "cluster-slave-1\n" > "$HADOOP_HOME/etc/hadoop/workers"
  if [ -f "$HADOOP_HOME/etc/hadoop/slaves" ]; then
    printf "cluster-slave-1\n" > "$HADOOP_HOME/etc/hadoop/slaves"
  fi
fi

$HADOOP_HOME/sbin/start-dfs.sh
$HADOOP_HOME/sbin/start-yarn.sh
mapred --daemon start historyserver
sleep 10
hdfs dfsadmin -safemode leave || true
hdfs dfs -mkdir -p /apps/spark/jars
hdfs dfs -mkdir -p /user/root
hdfs dfs -put -f /usr/local/spark/jars/* /apps/spark/jars/
jps -lm
hdfs dfsadmin -report
