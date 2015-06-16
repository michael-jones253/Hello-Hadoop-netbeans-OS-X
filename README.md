# Hello-Hadoop-netbeans-OS-X
Java Maven project for playing with the HDFS API without any 3rd party hadoop plugins.

## Aim of project
Hello world/tutorial level for programmatically operating with a pseudo distributed hadoop configuration on OS X. Hadoop 2.7 on Yosemite.

## What this project demonstrates

This is work in progress and so far the following functionality is demonstrated:

1. Direct programmatic control of the HDFS and running map reduce jobs - all unit tests run from the IDE with hadoop configuration built into this project and under git control.
2. Uploading of files to the HDFS by implementing a Web REST API client.
3. Async file upload (at time of writing only Jersey back end implemented).
4. Parallel/concurrent file upload of multiple files potentially to multiple data nodes.
 
All my unit tests demonstrating the above are running successfully on a pseudo-distributed Hadoop setup.

The following development principles:

1. Layered application code for maximum re-use and ease of test driven development. The client I wrote uses an interface which has allowed me to provide both a Jersey HTTP client implementation and an Apache HTTP client implementation for comparison between the two toolkits.
2. I tried to make the code easy to read.
3. Test driven development.

## The most interesting code

This is the REST concurrent (parallel on a multi-homed cluster) file upload from some storage server to the Hadoop data nodes. This code is working under unit test in a pseudo distributed configuration.

The easiest way to follow this code is to start with the unit test file HadoopHdfsRestClientTest.java and see how the following method is called. Then follow the layers through the interface down into the jersey client implementation.

Uploading to Hadoop via Web REST is a two stage process where first a PUT call to the name node is made to get a redirect location to the datanode. Then a PUT call to the datanode redirect location with the chunked file stream.

In the situation where the source files are going out on a single network connection one might wonder if anything can be gained from a concurrent file upload. However theoretically there is something to be gained, because some TCP file transfer protocols achieve speed by splitting files over multiple socket connections. So in the situation where we have multiple HTTP connections (one for each file), then we are achieving something similar.

The reason why multiple connections can be faster than one is because multiple connections achieve a quicker aggregate ramp up time during the TCP "slow start". On a good connection the gains are limited, because there will be only one ramp up at the start. However, across the internet under congestion slow starts may occur repeatedly during the duration of a connection. Another reason is that if some packet loss occurs, it is possible that the loss might only hit one connection in which case only one connection is throttled from the TCP congestion control.

The unit test for the above is in src/test/java/com/michaeljones/hellohadoop/restclient/HadoopHdfsRestClientTest.java.

The following excerpt is from /Users/michaeljones/NetBeansProjects/HelloHadoopWorldMaven/src/main/java/com/michaeljones/hellohadoop/restclient/HadoopHdfsRestClient.java

    public void ParallelUpload(List<Pair<String, String>> remoteLocalPairs) {
        // First of all do a concurrent gathering of the redirect locations from the name node.
        // Does this help if all calls are going to the same name node?
        // Answer: a little, because we are performing multiple round trips concurrently. Packets
        // are interleaved, but we are not individually blocking on each round trip interval.
        List<HttpMethodFuture> redirectFutures = new ArrayList();
        for (Pair<String, String> remoteLocal : remoteLocalPairs) {
            HttpMethodFuture redirect = GetRedirectLocationAsync(remoteLocal.getFirst(), remoteLocal.getSecond());
            redirectFutures.add(redirect);
        }

        // Now wait for all the redirect locations to arrive, by getting the result from the futures.
        // Make a new list of redirect location, local path pairs.
        // This will take as long as the longest redirect retrieval.
        List<Pair<String, String>> redirectLocalPairs = new ArrayList();
        for (HttpMethodFuture redirect : redirectFutures) {
            // This may block depending on whether any of the preceding futures took longer or not.
            String redirectLocation = redirect.GetRedirectLocation();
            String localPath = remoteLocalPairs.get(0).getSecond();
            redirectLocalPairs.add(new Pair<>(redirectLocation, localPath));
        }
        
        List<HttpMethodFuture> uploadFutures = new ArrayList();

        // The final step is to perform a concurrent/parallel upload to the data nodes.
        // In a multi-homed cluster these could be going out on separate network interfaces.
        // The bottleneck would then be the file storage. If some source paths were coming from
        // different file servers then we would really win with this strategy.
        for (Pair<String, String> redirectLocal : redirectLocalPairs) {
            HttpMethodFuture future = UploadFileAsync(redirectLocal.getFirst(), redirectLocal.getSecond());
            uploadFutures.add(future);
        }
        
        // Now wait for all uploads to complete, by getting the result from each future.
        // This will take as long as the longest upload.
        for (HttpMethodFuture upload : uploadFutures) {
            // This may block depending on whether any previous futures took longer or not.
            int httpStatusCode = upload.GetHttpStatusCode();
            if (httpStatusCode != 201) {
                // Should really log which file.
                LOGGER.error("Hadoop parallel upload unexpected status: " + httpStatusCode);                
            }
        }
    }


### Further discussions on concurrent transfer speed
I have made a call to set the HTTP chunk size to 1MB, however this is not an attempt to gain speed by reducing the payload overhead for the chunks. The payload overhead is quite small (only the size of the chunk) and there is no handshake for each chunk to slow things down. Instead the reason for doing this is the hope that the implementation uses a correspondingly large buffer size when writing the socket. The apache implementation hints that chunk size and buffer size are related, but I haven't verified this.

Having a large buffer size to write the socket is where transfer speed gains are made. If the socket read/write buffers are non-optimal then that is even more reason to upload over multiple TCP connections - several concurrent connections with a non-optimal implementation will be able to "fill the pipe".

### Design issues that need attending to.

1. I haven't considered timeouts yet. We must never block forever in the situation where the server has stopped reading the socket.
2. This demo program does not consider partial failure where one file fails to upload, it will give up on first error. This is not difficult to attend to, but needs some thought regarding what to do in this situation.
3. A limit on how many concurrent connections we allow for file transfer. There is a research paper on this.

## Getting started

The initial difficulty was working out how to run the program in the IDE and talk to the local pseudo distributed setup without using a 3rd party Hadoop plugin. Apparently there used to be a plugin for netbeans, but it has been discontinued. I am a newbie with this technology.

The Apache Hadoop instructions for building and running java programs are via their command line build/run utility only. Getting the yahoo hadoop HDFS tutorial program to build in the IDE wasn't too hard. However, by default it will ignore the local Hadoop configuration and only operate with the local file system in local debug mode.

## Wading through the configuration
Hadoop is designed to work with a number of different topologies, directory locations, levels of replication, simulated environments etc. so as we would expect nothing works unless the configuration is right.

First Hadoop needs to be installed and configured in pseudo distributed mode such that it builds and runs test programs on the command line. The hadoop documentation has instructions for doing this. Documentation is online and also included in the installation: share/doc/hadoop/index.html. However this is not enough to get it to work in the IDE.

## The netbeans project settings that are needed to run with the HDFS from the IDE:

1. Get log4j configured to work in the IDE. Absence of log4j configuration won't stop hadoop from running, but we won't get to see useful error messages from hadoop without it.
2. HADOOP_HOME environment variable project run (and test) property.
3. Duplicate hadoop configuration in the classpath. The above environment variable does not pick up the hadoop configuration from its usual place. Yahoo's only suggestion is to run from the command line via the hadoop script. The Apache HDFS API documention says that configuration is looked for in the classpath, so I took the approach of duplicating the configuration (just 2 files) instead of figuring out how to get it to look in the installation path.
4. The correct Maven dependencies. If not all the jar depenencies are there the program may build and run, but it won't talk to the HDFS if it doesn't load the HDFS jars.

I think that the setup of this project would probably work on Linux too.

## Working through trouble shooting the HDFS project settings

Issue 1 above shows up as:
<pre><code>
log4j:WARN No appenders could be found for logger (org.apache.hadoop.util.Shell).
log4j:WARN Please initialize the log4j system properly.
log4j:WARN See http://logging.apache.org/log4j/1.2/faq.html#noconfig for more info.
</pre></code>
This is solved by putting a log4j.properties configuration file in the classpath. For a Maven build getting files into the classpath can be achieved by putting them in src/main/resources. I don't think this is in the classpath, but the build will then copy the file into ./target/classes/ which is in the classpath.

Issue 2 above shows up as:
<pre><code>
Failed to detect a valid hadoop home directory
java.io.IOException: HADOOP_HOME or hadoop.home.dir are not set.
</code></pre>

This is fixed by going into the project properties => Build => Actions => Run Project and adding the environment variable for HADOOP_HOME. This will result in an entry in the Maven POM. Repeat for "Test Project", "Debug Project" etc.

Issue 3. shows up as running, but creating a file on the local file system instead of the HDFS. This was fixed       looking at the HDFS API documentation for org.apache.hadoop.conf.Configuration. This documentation is under the chapter: C API libhdfs HDFS which has a link to the HDFS API under "The APIs" (I strangely cannot find this in the main index). On my installation the HDFS API documentation link is file:///opt/local/hadoop-2.7.0/share/doc/hadoop/api/org/apache/hadoop/fs/FileSystem.html.

This documentation told me that hadoop loads core-site.xml and core-default.xml in the classpath. Classpath for a Maven build can be reached by placing these files in the directory src/main/resources (see logging configuration above). I added hdfs-site instead of core-default (which doesn't exist on my 2.7 install) into the src/main/resources project directory.


Issue 3 shows up as:
<pre><code>
java.io.IOException: "hadoop No FileSystem for scheme: hdfs".
</code></pre>

This is part of the build dependency problem. The various threads on stackoverflow etc. suggested that a minimum dependency was the jar for hadoop-core. However, there is no such jar on 2.7. I ran the hadoop command to dump version and got this:

<pre><code>
bin/hadoop version
Hadoop 2.7.0
Subversion https://git-wip-us.apache.org/repos/asf/hadoop.git -r d4c8d4d4d203c934e8074b31289a28724c0842cf
Compiled by jenkins on 2015-04-10T18:40Z
Compiled with protoc 2.5.0
From source with checksum a9e90912c37a35c3195d23951fd18f
This command was run using /opt/local/hadoop-2.7.0/share/hadoop/common/hadoop-common-2.7.0.jar
</code></pre>

I noted the jar name at the end and added it as a dependency to the POM. This can either be hand coded in or right click pom.xml => Insert Code => Add Dependency => Search => query hadoop-common and select version which was 2.7.0 [jar] - central. Maven will then add dependencies to other hadoop jars in the "Dependencies" group in the project explorer. Netbeans will then download these dependencies before the next build. They go into ~/.m2/repository should they ever need to be cleaned out (Netbeans doesn't seem to have an IDE remove dependency option).

After doing the above project built, but showed up the "no filesystem error". I noticed that Maven had not pulled in any hdfs jars matching the jars in my share/hadoop/hdfs installation directory. So I took a guess that it needed only the top level hadoop-hdfs-2.7.0.jar and added this as a dependency to the POM and all was good :)

## Working through trouble shooting the Map Reduce project settings
After getting the HDFS API working in the IDE it was time to move on to getting the famous Word Count map-reduce tutorial running.

So coded it, set the main program to the word count main as the one to run. From the IDE: project Properties => Run => Main Class. Provide input and output arguments for the word count program. I noted that the netbeans properties pop-up window does not persist these settings next time it pops up. However this setting is persistant and ends up in nbactions.xml.

However, running the word count map reduce program resulted in an ioException:

<pre><code>
Please check your configuration for mapreduce.framework.name and the correspond server addresses.
</pre></code>

Because the resources directory only contains 2 configuration files from my hadoop installation I thought maybe I was missing a map reduce one. So the first thing I did was go into the configuration directory of my installation and grep all files to see if there was a configuration file with mapreduce framework property. There was not. I checked my programatic configuration dump from my test program. This property was not mentioned either. The only map reduce configuration was to do with some environment variables for heap size in one of the shell scripts. Note to self: this may be relevant and I can consider setting this in the IDE. However, that didn't look like the problem.

I then checked my dependencies and noted there were no map reduce jars pulled in by Maven. So I added hadoop-mapreduce-client-core. Still not running. Checked the web and a thread on stackoverflow mentioned a number of other jars: hadoop-mapreduce-client-common and hadoop-mapreduce-client shuffle. I added just the hadoop-mapreduce-client-common and it runs :) I have a feeling that I might need some of the other jars for other API calls and that there must be a better way of working out dependencies than this trial and error. As mentioned I am a newbie with this, so if anyone has any comments feel free to email me.

## Logging from application code
Although the hadoop libraries log correctly with a properly located log4.properties file, application code calling the log4j logger seems to ignore this file. Using the log4j logger as per instructions in the log4j manual and many online tutorials did not result in getting any logging redirected to file configured in the properties file. Instead console output only is obtained. A clue to this mystery was that a most basic of hello world netbeans maven projects which did not import any hadoop stuff also failed to log to a file and did not even give the "No appenders" warning. Then I noticed that there were org.sl4j depencies in the hadoop build, so using sl4j instead solved the problem. Once my hello world project used sl4j's LoggerFactory and had sl4j-api and sl4j-12 as a dependencies it started to use the properties file. See http://www.slf4j.org/manual.html

## Upload a file from local storage to the HDFS via the REST API
The WebHDFS REST API documentation which came with the installation specifies that file creation is a two stage operation where an initial PUT to the namenode is sent. This responds with a redirect to the datanode and another PUT should be made to this redirection which can contain the chunked data stream. This does not sound REST to me as it relies on holding redirection state (a fully distributed configuration could presumably return a redirection to any one of the datanodes). Hadoop say this is because of a bug in some HTTP client implementations - incorrect implementation of “Expect: 100-continue”.

However, the 2 stage PUT not being REST seems to be a moot point, at least with the Jersey client. I do not get a redirection and a file creates/uploads all in one go.

Since writing the above paragraph I have implemented the Apache Client back end option to my HDFS Web client interface and this client behaves differently. It does indeed cause a redirect as per Hadoop documentation. The follow up PUT to the redirected location with the chunked file stream works as per documentation.

Since writing the above paragraph I have implemented a big chunk size setting for the HTTP chunking and this changed the behaviour of the Jersey client to always redirect. It might be that unless the chunk size option is set then the Jersey implementation does not chunk - this would be terrible for big files.

To be continued ...
