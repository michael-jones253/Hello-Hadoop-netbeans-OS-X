#!/bin/bash

# Source the prefix of our installation.
. mj_prefix
export HDFS_CMD="${MJ_PREFIX}/bin/hdfs"

export JAVA_HOME=`/usr/libexec/java_home`

echo "Hello World Bye World" > wcFile1.txt
echo "Hello Hadoop Goodbye Hadoop" > wcFile2.txt

$HDFS_CMD dfs -put wcFile1.txt  wcInput
$HDFS_CMD dfs -appendToFile wcFile2.txt  wcInput

exit 0
