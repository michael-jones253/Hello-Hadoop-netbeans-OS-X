/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.michaeljones.hellohadoopworldmaven;

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
        boolean expResult = true;
        boolean result = instance.isOnline();
        assertEquals(expResult, result);
    }

    /**
     * Test of writeFile method, of class HelloHdfs.
     */
    @org.junit.Test
    public void testWriteFile() {
        System.out.println("writeFile");
        HelloHdfs instance = new HelloHdfs();
        instance.writeFile();
        // TODO review the generated test code and remove the default call to fail.
        fail("The test case is a prototype.");
    }

    /**
     * Test of DumpConfig method, of class HelloHdfs.
     */
    @org.junit.Test
    public void testDumpConfig() {
        System.out.println("DumpConfig");
        HelloHdfs instance = new HelloHdfs();
        instance.DumpConfig();
        // TODO review the generated test code and remove the default call to fail.
        fail("The test case is a prototype.");
    }
    
}
