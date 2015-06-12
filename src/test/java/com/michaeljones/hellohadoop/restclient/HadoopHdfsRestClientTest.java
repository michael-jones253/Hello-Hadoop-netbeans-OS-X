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
        String host = "localhost";
        String username = "michaeljones";
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
        String host = "localhost";
        String username = "michaeljones";
        
        // First test the Jersey back end implementation.
        HadoopHdfsRestClient instance = HadoopHdfsRestClient.JerseyClientFactory(
                host,
                username);

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
        
        // Now test the Apache back end implementation. It should behave the same.
        instance = HadoopHdfsRestClient.ApacheClientFactory(host, username);
        directoryListing = instance.ListDirectorySimple(relativePath);
        assertTrue(directoryListing != null);
        assertTrue(directoryListing.length > 0);
        
        foundHelloFile = false;
        for (String listing : directoryListing) {
            if (listing.contentEquals("hello.txt")) {
                foundHelloFile = true;
            }
        }                
        // The file is expected to have been created by the HDFS tests.
        assertTrue(foundHelloFile);
    }

    /**
     * Test of CreateEmptyFile method, of class HadoopHdfsRestClient.
     */
    @Test
    public void testCreateEmptyFile() {
        System.out.println("CreateEmptyFile");
        String host = "localhost";
        String username = "michaeljones";

        HadoopHdfsRestClient instance = HadoopHdfsRestClient.JerseyClientFactory(
                host,
                username);

        String remoteRelativePath = "hello-jersey-emtpy.txt";
        instance.CreateEmptyFile(remoteRelativePath);
        
        // If it doesn't throw an exception, consider passed.
        assertTrue(true);

        // Create a different file for ease of HDFS verification.
        remoteRelativePath = "hello-apache-empty.txt";
        
        // Now test the Apache back end implementation. It should behave the same.
        instance = HadoopHdfsRestClient.ApacheClientFactory(host, username);
        instance.CreateEmptyFile(remoteRelativePath);
        // If it doesn't throw an exception, consider passed.
        assertTrue(true);
    }

    /**
     * Test of UploadFile method, of class HadoopHdfsRestClient.
     */
    @Test
    public void testUploadFile() {
        System.out.println("UploadFile");
        String host = "localhost";
        String username = "michaeljones";

        String remoteRelativePath = "hellohadoop.log";
        String localPath = "hellohadoop.log";
        HadoopHdfsRestClient instance = HadoopHdfsRestClient.JerseyClientFactory(
                host,
                username);

        instance.UploadFile(remoteRelativePath, localPath);

        // If it doesn't throw an exception, consider passed.
        assertTrue(true);
        
        // Create a different file for ease of HDFS verification.
        remoteRelativePath = "README.md";
        localPath = "README.md";
        
        // Now test the Apache back end implementation. It should behave the same.
        instance = HadoopHdfsRestClient.ApacheClientFactory(host, username);
        instance.UploadFile(remoteRelativePath, localPath);
        // If it doesn't throw an exception, consider passed.
        assertTrue(true);

    }

    /**
     * Test of ApacheClientFactory method, of class HadoopHdfsRestClient.
     */
    @Test
    public void testApacheClientFactory() {
        System.out.println("ApacheClientFactory");
        String host = "localhost";
        String username = "michaeljones";
        HadoopHdfsRestClient result = HadoopHdfsRestClient.ApacheClientFactory(host, username);
        
        // Not a lot to test.
        assertTrue(result != null);
    }
    
}
