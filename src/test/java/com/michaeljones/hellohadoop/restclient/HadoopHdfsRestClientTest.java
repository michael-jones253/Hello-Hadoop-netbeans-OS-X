/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.michaeljones.hellohadoop.restclient;

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
public class HadoopHdfsRestClientTest {
    
    public HadoopHdfsRestClientTest() {
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
     * Test of JerseyClientFactory method, of class HadoopHdfsRestClient.
     */
    @Test
    public void testJerseyClientFactory() {
        System.out.println("JerseyClientFactory");
        String host = "";
        String username = "";
        HadoopHdfsRestClient result = HadoopHdfsRestClient.JerseyClientFactory(host, username);
        
        // Not a lot to test.
        assertTrue(result != null);
    }

    /**
     * Test of ListDirectorySimple method, of class HadoopHdfsRestClient.
     */
    @Test
    public void testListDirectorySimple() {
        System.out.println("ListDirectory");
        HadoopHdfsRestClient instance = HadoopHdfsRestClient.JerseyClientFactory(
                "localhost",
                "michaeljones");

        // Home directory.
        String relativePath = "";

        String[] directoryListing = instance.ListDirectorySimple(relativePath);
        assertTrue(directoryListing != null);
        assertTrue(directoryListing.length > 0);
        
        boolean foundHelloFile = false;
        for (String listing : directoryListing) {
            if (listing.contentEquals("hello.txt")) {
                foundHelloFile = true;
            }
        }
        
        // The file is expected to have been created by the HDFS tests.
        assertTrue(foundHelloFile);
    }
    
}
