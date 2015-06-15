/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.michaeljones.httpclient.apache;

import com.michaeljones.hellohadoopworldmaven.HelloHdfsTest;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.math3.util.Pair;
import org.junit.After;
import org.junit.AfterClass;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author michaeljones
 */
public class ApacheJsonMethodTest {
        // Send the logs to the same appender as for the hellohadoopworld package.
    private static final Logger LOGGER = LoggerFactory.getLogger(HelloHdfsTest.class.getName());
    
    public ApacheJsonMethodTest() {
    }
    
    @BeforeClass
    public static void setUpClass() {
        // HDFS needs to be running, because the following URI is a Web HDFS resource.

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
     * Test of GetStringContent method, of class ApacheJsonMethod.
     */
    @Test
    public void testGetStringContent() {
        System.out.println("GetStringContent");

        // Test list a directory on the HDFS.
        String url = "http://localhost:50070/webhdfs/v1/user/michaeljones/";
        List<Pair<String, String>> queryParams = new ArrayList();
        
        // List does not require username and password query parameters.
        queryParams.add(new Pair<>("op","LISTSTATUS"));
        ApacheJsonMethod instance = new ApacheJsonMethod();
        String result = instance.GetStringContent(url, queryParams);
        
        LOGGER.info("Apache client testGetStringContent");
        LOGGER.info(result);

        assertTrue(result.contains("FileStatus"));
    }

    /**
     * Test of PutQuery method, of class ApacheJsonMethod.
     */
    @Test
    public void testPutQuery() {
        System.out.println("PutQuery");
        // Test creation of empty file on HDFS.
        String url = "http://localhost:50070/webhdfs/v1/user/michaeljones/apache-empty.txt";
        List<Pair<String, String>> queryParams = new ArrayList();
        queryParams.add(new Pair<>("user.name","michaeljones"));
        queryParams.add(new Pair<>("op","CREATE"));
        queryParams.add(new Pair<>("overwrite","true"));

        ApacheJsonMethod instance = new ApacheJsonMethod();
        
        // The Apache and Jersey clients behave differently. Jersey appears to implement
        // the "Expect: 100-continue” header correctly, but some clients (like Apache)
        // will cause a redirect. See hadoop documentation for the following workaround.
        int expTempRedirectResult = 307;
        StringBuilder redirectLocation = new StringBuilder();
        
        // Run the first PUT on the name node to get a redirect location.
        int result = instance.PutQuery(url, queryParams, redirectLocation);
        assertEquals(expTempRedirectResult, result);
        assertTrue(redirectLocation.toString().length() > 0);
        LOGGER.info("PUT file redirect: " + redirectLocation.toString());
        
        // Run the second PUT on the redirect location. This location is a data node URI.
        int expCreatedResult = 201;
        result = instance.PutQuery(redirectLocation.toString(), null, null);
        assertEquals(result, expCreatedResult);
    }

    /**
     * Test of PutFile method, of class ApacheJsonMethod.
     * @throws java.lang.Exception
     */
    @Test
    public void testPutFile() throws Exception {
        System.out.println("PutFile");
        // Test copy of local file to HDFS.
        String url = "http://localhost:50070/webhdfs/v1/user/michaeljones/pom.xml";
        List<Pair<String, String>> queryParams = new ArrayList();
        queryParams.add(new Pair<>("user.name","michaeljones"));
        queryParams.add(new Pair<>("op","CREATE"));
        queryParams.add(new Pair<>("overwrite","true"));

        ApacheJsonMethod instance = new ApacheJsonMethod();
                        
        // Will cause a redirect. See hadoop documentation for the following workaround.
        int expTempRedirectResult = 307;
        StringBuilder redirectLocation = new StringBuilder();

        // Run the first PUT on the name node to get a redirect location.
        int result = instance.PutQuery(url, queryParams, redirectLocation);
        assertEquals(expTempRedirectResult, result);
        assertTrue(redirectLocation.toString().length() > 0);
        LOGGER.info("PUT file redirect: " + redirectLocation.toString());
        
        // Run the second PUT on the redirect location. This location is a data node URI.
        String localFilePath= "pom.xml";
        int expCreatedResult = 201;
        result = instance.PutFile(redirectLocation.toString(), localFilePath, null, null);
        assertEquals(result, expCreatedResult);    
    }

    /**
     * Test of Close method, of class ApacheJsonMethod.
     */
    @Test
    public void testClose() {
        System.out.println("Close");
        ApacheJsonMethod instance = new ApacheJsonMethod();
        instance.Close();
        
        // If it didn't throw then consider passed.
        assertTrue(true);
    }
    
}
