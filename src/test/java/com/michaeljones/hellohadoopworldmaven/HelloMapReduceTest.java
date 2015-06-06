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
    static final String wcOutputAnalysisPathDir = "wcOutputAnalysis";
    static final String wcOutputMainPathDir = "wcOutputMain";
    static Configuration hadoopConfig = null;
    
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
        FileSystem hdfs = FileSystem.get(hadoopConfig);
        Path outputPath = new Path(wcOutputPathDir);

        // We need to remove the output directory before running the map reduce job.
        if (hdfs.exists(outputPath)) {
            // remove the directory recursively.
            hdfs.delete(outputPath, true);
        }

        Path inputPath = new Path(wcInputPathDir);
        Job result = HelloMapReduce.RunJobAsync(inputPath, outputPath, hadoopConfig);
        boolean ok = result.waitForCompletion(true);
        assertTrue(ok);
    }
    
    /**
     * Test of RunJobAnalysisAsync method, of class HelloMapReduce.
     * @throws java.lang.Exception
     */
    @Test
    public void testRunJobAnalysisAsync() throws Exception {
        System.out.println("RunJobAnalysisAsync");
        FileSystem hdfs = FileSystem.get(hadoopConfig);
        Path outputPath = new Path(wcOutputAnalysisPathDir);
        if (hdfs.exists(outputPath)) {
            hdfs.delete(outputPath, true);
        }
        
        Path inputPath = new Path(wcInputPathDir);
        Job result = HelloMapReduce.RunJobAnalysisAsync(inputPath, outputPath, hadoopConfig);
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
        FileSystem hdfs = FileSystem.get(hadoopConfig);

        Path outputPath = new Path(wcOutputMainPathDir);
        if (hdfs.exists(outputPath)) {
            hdfs.delete(outputPath, true);
        }
        
        String[] args = {wcInputPathDir, wcOutputMainPathDir};
        HelloMapReduce.main(args);
        
        // Assume it is true.
        assertTrue(true);
    }
    
}
