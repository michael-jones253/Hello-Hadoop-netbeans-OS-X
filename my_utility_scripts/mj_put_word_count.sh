#!/bin/bash
export JAVA_HOME=`/usr/libexec/java_home`

echo "Hello World Bye World" > wcFile1.txt
echo "Hello Hadoop Goodbye Hadoop" > wcFile2.txt

bin/hdfs dfs -put wcFile1.txt  wcInput
bin/hdfs dfs -appendToFile wcFile2.txt  wcInput

exit 0
