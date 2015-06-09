This directory does not bother to supply the "slaves" file. The slaves
file lists all the slave datanodes and in pseudo distributed configuration
contains just the line "localhost". The slaves file is only needed by the
master node (I think).

The reason why this IDE environment does not maintain a copy of the slaves
file is because none of the hadoop java looks at it (according to the Hadoop
Cluster Setup chapter of the documentation).

It is actually used by the start-dfs.sh script which starts up namenode
and datanodes all in one go from the master node.
See my_utility_scripts/mj_start_pseudo_distrib.sh script in this netbeans
project for its use.

An alternative to calling start-dfs.sh is to individually start up the name
and data nodes using the hdfs command with either "start datanode" or
"start namenode" parameters. The command needs to be run on each of the node
machines.

The "stop all nodes" script also uses the slaves file.
See my_utility_scripts/mj_stop.sh which calls this stop script.

In a non-yarn setup I think the master node is synonmyous with the namenode.
