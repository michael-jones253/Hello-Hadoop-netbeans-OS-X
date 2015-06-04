#!/bin/bash

export JAVA_HOME=`/usr/libexec/java_home`

# Source the prefix of our installation.
. mj_prefix

export HDFS_CMD="${MJ_PREFIX}/bin/hdfs"
export START_CMD="${MJ_PREFIX}/sbin/start-dfs.sh"

# This script starts up the nodes for pseudo distributed confuration.

# Just some limited checks to see we have the configuration.
NAMENODE_URI=`$HDFS_CMD getconf -confKey fs.defaultFS 2> /dev/null`
HAS_REPLICATION=`$HDFS_CMD getconf -confKey dfs.replication 2> /dev/null`

HAS_NAMENODE=`$HDFS_CMD getconf -namenodes 2> /dev/null`
HAS_SECONDARY_NAMENODE=`$HDFS_CMD getconf -secondaryNameNodes 2> /dev/null`
HAS_BACKUPNODE=`$HDFS_CMD getconf -backupnodes 2> /dev/null`

if [ "${NAMENODE_URI}" != "hdfs://localhost:9000" ]; then
    echo "Check HDFS is localhost in etc/hadoop/core-site.xml configuration"
    exit -1
fi

if [ "${HAS_REPLICATION}" != "1" ]; then
    echo "Check replication in etc/hadoop/hdfs-site.xml configuration"
    exit -1
fi

# When the hdfs is stored in /tmp
# Formatting needs to be done EVERY time we lose /tmp/hadoop-<username>
# or the configured storage directory.
NAMENODE_DIR=`$HDFS_CMD getconf -confKey dfs.namenode.name.dir 2> /dev/null`

if [[ ${NAMENODE_DIR} == file://* ]]; then
    NAMENODE_DIR=`echo ${NAMENODE_DIR} | cut -c 7-`
fi

NAMENODE_STORAGE_VERSION_FILE="${NAMENODE_DIR}/current/VERSION"

if [ ! -f "${NAMENODE_STORAGE_VERSION_FILE}" ]; then
    echo "Format HDFS on ${NAMENODE_STORAGE_VERSION_FILE}"
    echo -n "OK to proceed? [y/n]: "
    read ANSWER
    if [ $ANSWER = "y" ]; then
        $HDFS_CMD namenode -format
    else
        echo "Aborting, fix up name node storage configuration and re-try."
        exit 1
    fi
fi

$START_CMD

WHOIAM=`whoami`

HDFS_USERS=`$HDFS_CMD dfs -ls /user/${WHOIAM} | grep "${WHOIAM}" 2> /dev/null`

if [ "${HDFS_USERS}" = "" ]; then
    # Making the directories to hold the input/output needs to be done
    # every time we lose our local hadoop storage. This will be in /tmp
    # unless we have configured dfs.namenode.name.dir and
    # dfs.datanode.data.dir in hdfs-site.xml. We can configure these
    # with pseudo distributed if we want.

    $HDFS_CMD dfs -mkdir /user 2> /dev/null
    $HDFS_CMD dfs -mkdir "/user/${WHOIAM}" 2> /dev/null
else
    echo "${WHOIAM} exists: ${HDFS_USERS}"
fi

echo "Namenode URI: ${NAMENODE_URI}"
echo "Replication: ${HAS_REPLICATION}"

echo "Namenode: ${HAS_NAMENODE}"
echo "Secondary Namenode: ${HAS_SECONDARY_NAMENODE}"

echo "to stop: mj_stop.sh"
exit 0
