/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.michaeljones.hellohadoop.restclient;

import com.michaeljones.hellohadoopworldmaven.HelloHdfsTest;
import com.michaeljones.httpclient.HttpMethodFuture;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.math3.util.Pair;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author michaeljones
 */
public class HadoopHdfsRestClientTest {
    private static final String NAMENODE_HOST = "localhost";
    private static final String USERNAME = "michaeljones";

    // Send the logs to the same appender as for the hellohadoopworld package.
    private static final Logger LOGGER = LoggerFactory.getLogger(HelloHdfsTest.class.getName());

    
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
        HadoopHdfsRestClient result = HadoopHdfsRestClient.JerseyClientFactory(NAMENODE_HOST, USERNAME);
        
        // Not a lot to test.
        assertTrue(result != null);
    }

    /**
     * Test of ListDirectorySimple method, of class HadoopHdfsRestClient.
     */
    @Test
    public void testListDirectorySimple() {
        System.out.println("ListDirectory");
        
        // First test the Jersey back end implementation.
        HadoopHdfsRestClient instance = HadoopHdfsRestClient.JerseyClientFactory(NAMENODE_HOST, USERNAME);

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
        instance = HadoopHdfsRestClient.ApacheClientFactory(NAMENODE_HOST, USERNAME);
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

        HadoopHdfsRestClient instance = HadoopHdfsRestClient.JerseyClientFactory(NAMENODE_HOST, USERNAME);

        String remoteRelativePath = "hello-jersey-emtpy.txt";
        instance.CreateEmptyFile(remoteRelativePath);
        
        // If it doesn't throw an exception, consider passed.
        assertTrue(true);

        // Create a different file for ease of HDFS verification.
        remoteRelativePath = "hello-apache-empty.txt";
        
        // Now test the Apache back end implementation. It should behave the same.
        instance = HadoopHdfsRestClient.ApacheClientFactory(NAMENODE_HOST, USERNAME);
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

        String remoteRelativePath = "hellohadoop.log";
        String localPath = "logs/archive/hellohadoop.log";
        HadoopHdfsRestClient instance = HadoopHdfsRestClient.JerseyClientFactory(NAMENODE_HOST, USERNAME);

        instance.UploadFile(remoteRelativePath, localPath);

        // If it doesn't throw an exception, consider passed.
        assertTrue(true);
        
        // Now test with setting a big chunk size.
        instance.SetBigChunkSize();
        
        // Create a different file for ease of HDFS verification.
        remoteRelativePath = "hello.log.2015-06-12";
        localPath = "logs/archive/hello.log.2015-06-12";
        
        instance.UploadFile(remoteRelativePath, localPath);

        // If it doesn't throw an exception, consider passed.
        assertTrue(true);
        
        // Create a different file for ease of HDFS verification.
        remoteRelativePath = "README.md";
        localPath = "README.md";
        
        // Now test the Apache back end implementation. It should behave the same.
        instance = HadoopHdfsRestClient.ApacheClientFactory(NAMENODE_HOST, USERNAME);
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
        HadoopHdfsRestClient result = HadoopHdfsRestClient.ApacheClientFactory(NAMENODE_HOST, USERNAME);
        
        // Not a lot to test.
        assertTrue(result != null);
    }

    /**
     * Test of GetRedirectLocationAsync method, of class HadoopHdfsRestClient.
     */
    @Test
    public void testGetRedirectLocationAsync() {
        System.out.println("GetRedirectLocationAsync");
        String remoteRelativePath = "nbactions.xml";
        String localPath = "nbactions.xml";
        HadoopHdfsRestClient instance = HadoopHdfsRestClient.JerseyClientFactory(NAMENODE_HOST, USERNAME);
        instance.SetBigChunkSize();
        
        HttpMethodFuture futureResult = instance.GetRedirectLocationAsync(remoteRelativePath, localPath);
        String redirectLocation = futureResult.GetRedirectLocation();
        // Check that the redirected URI is for the data node port.
        assertTrue(redirectLocation.contains("50075"));

        // Check that the redirected URI has query param for create.
        assertTrue(redirectLocation.contains("op=CREATE"));
        
        assertTrue(redirectLocation.contains(remoteRelativePath));
    }

    /**
     * Test of UploadFileAsync method, of class HadoopHdfsRestClient.
     */
    @Test
    public void testUploadFileAsync() {
        System.out.println("UploadFileAsync");
        String localPath = "nbactions.xml";
        String remoteRelativePath = "nbactions.xml";

        HadoopHdfsRestClient instance = HadoopHdfsRestClient.JerseyClientFactory(NAMENODE_HOST, USERNAME);
        instance.SetBigChunkSize();
        
        // First of all get the redirect location from the name node.
        HttpMethodFuture futureResult = instance.GetRedirectLocationAsync(remoteRelativePath, localPath);
        String redirectLocation = futureResult.GetRedirectLocation();
        
        assertTrue(redirectLocation.contains("50075"));
        LOGGER.info("testUploadFileAsync: redirect: " + redirectLocation);
        
        // Now feed the redirected data node location into the upload.
        HttpMethodFuture result = instance.UploadFileAsync(redirectLocation, localPath);
        
        int expCreatedCode = 201;
        assertTrue(result.GetHttpStatusCode() == expCreatedCode);
    }

    /**
     * Test of SetBigChunkSize method, of class HadoopHdfsRestClient.
     */
    @Test
    public void testSetBigChunkSize() {
        System.out.println("SetBigChunkSize");
        HadoopHdfsRestClient instance = HadoopHdfsRestClient.JerseyClientFactory(NAMENODE_HOST, USERNAME);
        instance.SetBigChunkSize();

        // Not much to verify. If not thrown assume pass.
        assertTrue(true);
    }

    /**
     * Test of ParallelUpload method, of class HadoopHdfsRestClient.
     */
    @Test
    public void testParallelUpload() {
        System.out.println("ParallelUpload");
        List<Pair<String, String>> remoteLocalPairs = new ArrayList();
        // Upload some log files, destination file name same as source.
        remoteLocalPairs.add(new Pair<>("hellohadoop.log.2015-06-09", "logs/archive/hellohadoop.log.2015-06-09"));
        remoteLocalPairs.add(new Pair<>("hellohadoop.log.2015-06-10", "logs/archive/hellohadoop.log.2015-06-10"));
        remoteLocalPairs.add(new Pair<>("hellohadoop.log.2015-06-11", "logs/archive/hellohadoop.log.2015-06-11"));
        remoteLocalPairs.add(new Pair<>("hellohadoop.log.2015-06-12", "logs/archive/hellohadoop.log.2015-06-12"));
        remoteLocalPairs.add(new Pair<>("hellohadoop.log.2015-06-13", "logs/archive/hellohadoop.log.2015-06-13"));
        
        HadoopHdfsRestClient instance = HadoopHdfsRestClient.JerseyClientFactory(NAMENODE_HOST, USERNAME);
        instance.SetBigChunkSize();

        instance.ParallelUpload(remoteLocalPairs);
        
        // If we didn't throw assume pass.
        assertTrue(true);    
    }

    /**
     * Test of DeleteFile method, of class HadoopHdfsRestClient.
     */
    @Test
    public void testDeleteFile() {
        System.out.println("DeleteFile");
        String remoteRelativePath = "hellohadoop.log";
        HadoopHdfsRestClient instance = HadoopHdfsRestClient.JerseyClientFactory(NAMENODE_HOST, USERNAME);
        instance.DeleteFile(remoteRelativePath);
        
        instance = HadoopHdfsRestClient.ApacheClientFactory(NAMENODE_HOST, USERNAME);
        instance.DeleteFile(remoteRelativePath);
        
        // If we didn't throw assume pass.
        assertTrue(true);    
    }
    
}
