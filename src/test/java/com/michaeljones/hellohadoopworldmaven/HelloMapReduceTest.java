/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.michaeljones.hellohadoopworldmaven;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 * @author michaeljones
 */
public class HelloMapReduceTest {
    static final String wcInputPathDir = "wcInput";
    static final String wcOutputPathDir = "wcOutput";
    Configuration hadoopConfig = null;
    
    public HelloMapReduceTest() {
        hadoopConfig = new Configuration();
    }
    
    @BeforeClass
    public static void setUpClass() {
    }
    
    @AfterClass
    public static void tearDownClass() {
    }
    
    @Before
    public void setUp() {
        try {
            FileSystem hdfs = FileSystem.get(hadoopConfig);
            Path outputPath = new Path(wcOutputPathDir);

            // We need to remove the output directory before running the map reduce job.
            if (hdfs.exists(outputPath)) {
                // remove the directory recursively.
                hdfs.delete(outputPath, true);
            }

        } catch (IOException ex) {
            Logger.getLogger(HelloMapReduceTest.class.getName()).log(Level.SEVERE, null, ex);
            fail("Cannot setup test.");
        }
    }
    
    @After
    public void tearDown() {
    }

    /**
     * Test of RunJobAsync method, of class HelloMapReduce.
     * @throws java.lang.Exception
     */
    @Test
    public void testRunJobAsync() throws Exception {
        System.out.println("RunJobAsync");
        Path inputPath = new Path(wcInputPathDir);
        Path outputPath = new Path(wcOutputPathDir);
        Job result = HelloMapReduce.RunJobAsync(inputPath, outputPath, hadoopConfig);
        boolean ok = result.waitForCompletion(true);
        assertTrue(ok);
    }

    /**
     * Test of main method, of class HelloMapReduce.
     * @throws java.lang.Exception
     */
    @Test
    public void testMain() throws Exception {
        System.out.println("main");
        String[] args = {wcInputPathDir, wcOutputPathDir};
        HelloMapReduce.main(args);
    }
    
}
