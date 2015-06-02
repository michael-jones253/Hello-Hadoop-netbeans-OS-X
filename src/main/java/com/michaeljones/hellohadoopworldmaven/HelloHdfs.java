/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.michaeljones.hellohadoopworldmaven;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
// import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;

/**
 *
 * @author michaeljones
 */
public class HelloHdfs {

    public static final String theFilename = "hello.txt";
    public static final String message = "Hello HDFS world!\n";

    public void WriteFile() {
        Configuration hadoopConfig = new Configuration();

        try {
            // FIX for java.io.IOException: "hadoop No FileSystem for scheme: hdfs".
            // Maven or its dependencies didn't do a perfect job and I had to add
            // a dependency for the hadoop-hdf jar in the POM to get the following
            // to compile.
            // The following lines came from: http://stackoverflow.com/questions/17265002/hadoop-no-filesystem-for-scheme-file
            // It is possible that adding the equivalent to core-site.xml will work too - need to try that.
            hadoopConfig.set("fs.hdfs.impl",
                    org.apache.hadoop.hdfs.DistributedFileSystem.class.getName()
            );
            hadoopConfig.set("fs.file.impl",
                    org.apache.hadoop.fs.LocalFileSystem.class.getName()
            );
            
            FileSystem fs = FileSystem.get(hadoopConfig);

            Path filenamePath = new Path(theFilename);

            if (fs.exists(filenamePath)) {
                // remove the file first
                fs.delete(filenamePath);
            }

            FSDataOutputStream out = fs.create(filenamePath);
            out.writeUTF(message);
            out.close();

            FSDataInputStream in = fs.open(filenamePath);
            String messageIn = in.readUTF();
            System.out.print(messageIn);
            in.close();
        } catch (IOException ioe) {
            System.err.println("IOException during operation: " + ioe.toString());
            System.exit(1);
        }
    }
}
