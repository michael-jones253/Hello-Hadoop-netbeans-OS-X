# Hello-Hadoop-netbeans-OS-X
Java Maven project for playing with the HDFS API without any 3rd party hadoop plugins.

## Aim of project
Just hello world tutorial level work in progress for programmatically operating with a pseudo distributed hadoop configuration on OS X. Hadoop 2.7 on Yosemite.

So far the difficult part has been working out how to run the program in the IDE and talk to the local pseudo distributed setup without using a 3rd party Hadoop plugin. Apparently there used to be a plugin for netbeans, but it has been discontinued. I am a newbie with this technology.

The Apache Hadoop instructions for building and running java programs are via their command line build/run utility only. Getting the yahoo hadoop HDFS tutorial program to build in the IDE wasn't too hard. However, by default it will ignore the local Hadoop configuration and only operate with the local file system in local debug mode.

## Wading through the configuration.
Hadoop is designed to work with a number of different topologies, directory locations, levels of replication, simulated environments etc. so as we would expect nothing will work unless the configuration is right.

The things that are needed to run with the HDFS from the IDE:

1. HADOOP_HOME environment variable project run property.
2. Duplicate hadoop configuration in the classpath. The above environment variable does not pick up the hadoop configuration from its usual place. Yahoo suggests running from the command line only with the hadoop script. The Apache HDFS API documention says that configuration is looked for in the classpath, so I took this approach.
3. The correct Maven dependencies. If not all the jar depenencies are there the program may build and run, but it won't talk to the HDFS if it doesn't load the HDFS jars.

I think that the setup of this project would probably work on Linux too.

To be continued ...
