#!/bin/bash

# If this script runs and prints the version, then the other scripts
# should run too.

# Source the prefix of our particular installation. Edit the following
# file if it is wrong.
. mj_prefix

export HDFS_CMD="${MJ_PREFIX}/bin/hdfs"

$HDFS_CMD version

exit 0
