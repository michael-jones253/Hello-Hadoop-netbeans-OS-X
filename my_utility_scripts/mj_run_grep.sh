#!/bin/bash
export JAVA_HOME=`/usr/libexec/java_home`

bin/hdfs dfs -put etc/hadoop input2

bin/hadoop jar share/hadoop/mapreduce/hadoop-mapreduce-examples-2.7.0.jar grep input2 output2 'dfs[a-z.]+' 2>&1| tee grep_2_out.text

bin/hdfs dfs -cat output2/*

exit 0
