#!/bin/bash
export JAVA_HOME=`/usr/libexec/java_home`

bin/hadoop jar WordCount-mapreduce.jar WordCount wcInput wcOutput 2>&1| tee wc_out.text

bin/hdfs dfs -cat wcOutput/*

exit 0
