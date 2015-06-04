/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.michaeljones.hellohadoopworldmaven;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;
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
public class HelloHdfsTest {
    
    public HelloHdfsTest() {
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
     * Test of isOnline method, of class HelloHdfs.
     */
    @org.junit.Test
    public void testIsOnline() {
        System.out.println("isOnline");
        HelloHdfs instance = new HelloHdfs();
        
        // This test expects the HDFS to be running.
        assertTrue(instance.isOnline());
    }

    /**
     * Test of writeFile method, of class HelloHdfs.
     */
    @org.junit.Test
    public void testWriteFile() {
        System.out.println("writeFile");
        HelloHdfs instance = new HelloHdfs();
        try {
            instance.writeFile();
        } catch (IOException ex) {
            Logger.getLogger(HelloHdfsTest.class.getName()).log(Level.SEVERE, null, ex);
            fail("Exception thrown.");
        }
    }

    /**
     * Test of DumpConfig method, of class HelloHdfs.
     */
    @org.junit.Test
    public void testCheckForDeprecatedConfig() {
        System.out.println("DumpConfig");
        HelloHdfs instance = new HelloHdfs();

        boolean dumpAll = true;
        assertFalse(instance.checkForDeprecatedConfig(dumpAll));
    }
    
}
