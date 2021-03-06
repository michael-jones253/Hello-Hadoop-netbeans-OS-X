/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.michaeljones.hellohadoopworldmaven;

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

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
    public static final String configDumpFileName = "output/configurationDump.txt";
    public static final String theFilename = "hello.txt";
    public static final String message = "Hello HDFS world!\n";
    private static final Logger LOGGER = Logger.getLogger(HelloHdfs.class.getName());

    Configuration hadoopConfig;
    FileSystem hdfs;

    private boolean hdfsIsOnline = false;
    
    public HelloHdfs() {
        try {
            // The HDFS API documentation for org.apache.hadoop.conf.Configuration told
            // me that it needs to load core-site.xml and core-default.xml in the
            // classpath. Classpath for Maven build is src/main/resources. I added
            // hdfs-site instead of core-default (which doesn't exist on my 2.7 install).
            hadoopConfig = new Configuration();
            
            hdfs = FileSystem.get(hadoopConfig);
            
            switch (hdfs.getScheme()) {
                case "hdfs":
                    hdfsIsOnline = true;
                    break;
                case "file":
                    break;
                default:
                    LOGGER.log(Level.WARNING,
                            "Unknown filesystem scheme: {0}", hdfs.getScheme());
                    break;
            }            
        } catch (IOException ex) {
            LOGGER.log(Level.SEVERE, null, ex);
        }
        
    }
    
    public boolean isOnline() {
        return hdfsIsOnline;
    }

    public void writeFile() throws IOException {

        try {            
            Path filenamePath = new Path(theFilename);

            if (hdfs.exists(filenamePath)) {
                // remove the file first
                hdfs.delete(filenamePath);
            }

            FSDataOutputStream out = hdfs.create(filenamePath);
            out.writeUTF(message);
            out.close();

            FSDataInputStream in = hdfs.open(filenamePath);
            String messageIn = in.readUTF();
            LOGGER.log(Level.INFO, messageIn);

            in.close();
        } catch (IOException ioe) {
            LOGGER.log(Level.SEVERE, "IOException during writeFile.", ioe);
            throw ioe;
        }
    }
    
    public boolean checkForDeprecatedConfig(boolean dumpAll) {
        PrintWriter dumpOut = null;
        boolean hasDeprecation = false;
        try {
            dumpOut = new PrintWriter( new BufferedWriter(new FileWriter(configDumpFileName)));
            Iterator<Map.Entry<String, String>> iterator = hadoopConfig.iterator();
            while (iterator.hasNext()) {
                Map.Entry<String, String> entry = iterator.next();
                String key = entry.getKey();
                String value = entry.getValue();
                boolean deprecated = Configuration.isDeprecated(key);
                if (deprecated) {
                    hasDeprecation = true;
                    dumpOut.println("Key: " + key + "Value: " + value + " DEPRECATED.");
                }
                else if (dumpAll) {
                    dumpOut.println("Key: " + key + "Value: " + value);
                }
            }
        } catch (FileNotFoundException ex) {
            Logger.getLogger(HelloHdfs.class.getName()).log(Level.SEVERE, null, ex);
        } catch (IOException ex) {
            Logger.getLogger(HelloHdfs.class.getName()).log(Level.SEVERE, null, ex);
        }
        finally {
            if (dumpOut != null) {
                dumpOut.close();
            }
        }
        
        return hasDeprecation;
    }
}
