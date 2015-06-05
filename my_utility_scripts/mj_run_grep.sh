#!/bin/bash

# Source the prefix of our installation.
. mj_prefix
export HADOOP_CMD="${MJ_PREFIX}/bin/hadoop"
export HDFS_CMD="${MJ_PREFIX}/bin/hdfs"

export JAVA_HOME=`/usr/libexec/java_home`

$HDFS_CMD dfs -put etc/hadoop input2

$HADOOP_CMD jar share/hadoop/mapreduce/hadoop-mapreduce-examples-2.7.0.jar grep input2 output2 'dfs[a-z.]+' 2>&1| tee grep_2_out.text

bin/hdfs dfs -cat output2/*

exit 0
