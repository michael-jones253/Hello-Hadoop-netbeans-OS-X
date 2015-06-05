#!/bin/bash

# Script to compile a java hadoop map-reduce program.
# Output is a jar file which can be run on the command line
# via the hadoop executable.

# Source the prefix of our installation.
. mj_prefix
export HADOOP_CMD="${MJ_PREFIX}/bin/hadoop"

export JAVA_HOME=`/usr/libexec/java_home`
export PATH=${JAVA_HOME}/bin:${PATH}
export HADOOP_CLASSPATH=${JAVA_HOME}/lib/tools.jar


echo $JAVA_HOME
echo $PATH
echo $HADOOP_CLASSPATH

if [ "$1" == "" ]; then
    echo "Please supply Java program name without the .java postfix."
    exit -1
fi

PROG_NAME=$1

echo "Compiling ${PROG_NAME}.java"

$HADOOP_CMD com.sun.tools.javac.Main ${PROG_NAME}.java

PROG_CLASSES=${PROG_NAME}*.class

echo "Jar-ing:"
ls ${PROG_CLASSES}

jar cf ${PROG_NAME}-mapreduce.jar ${PROG_CLASSES}

exit 0
