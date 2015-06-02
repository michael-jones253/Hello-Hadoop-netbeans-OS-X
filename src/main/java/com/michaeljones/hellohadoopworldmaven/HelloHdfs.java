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
        // The HDFS API documentation for org.apache.hadoop.conf.Configuration told
        // me that it needs to load core-site.xml and core-default.xml - I added
        // hdfs-site instead of core-default (which doesn't exist on my 2.7 install.
        Configuration hadoopConfig = new Configuration();

        try {            
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
