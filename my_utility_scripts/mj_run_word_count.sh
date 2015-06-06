#!/bin/bash
# Source the prefix of our installation.
. mj_prefix
export HADOOP_CMD="${MJ_PREFIX}/bin/hadoop"
export HDFS_CMD="${MJ_PREFIX}/bin/hdfs"

export JAVA_HOME=`/usr/libexec/java_home`

$HADOOP_CMD jar WordCount-mapreduce.jar WordCount wcInput wcOutput 2>&1| tee wc_out.out

$HDFS_CMD dfs -cat wcOutput/*

exit 0
