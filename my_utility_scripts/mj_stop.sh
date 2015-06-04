#!/bin/bash

export JAVA_HOME=`/usr/libexec/java_home`

# Source the prefix of our installation.
. mj_prefix

export STOP_HDFS_CMD="${MJ_PREFIX}/sbin/stop-dfs.sh"

$STOP_HDFS_CMD

exit 0
