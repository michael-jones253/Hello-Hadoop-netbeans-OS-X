/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.michaeljones.httpclient.jersey;

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
public class JerseyJsonMethodTest {
        // Send the logs to the same appender as for the hellohadoopworld package.
    private static final Logger LOGGER = LoggerFactory.getLogger(HelloHdfsTest.class.getName());

    
    public JerseyJsonMethodTest() {
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
     * Test of GetStringContent method, of class JerseyJsonMethod.
     */
    @Test
    public void testGetStringContent() {
        System.out.println("GetStringContent");
        
        // Test list a directory on the HDFS.
        String url = "http://localhost:50070/webhdfs/v1/user/michaeljones/";
        List<Pair<String, String>> queryParams = new ArrayList();
        
        // List does not require username and password query parameters.
        queryParams.add(new Pair<>("op","LISTSTATUS"));
        JerseyJsonMethod instance = new JerseyJsonMethod();
        String result = instance.GetStringContent(url, queryParams);
        
        LOGGER.info("Jersey client testGetStringContent");
        LOGGER.info(result);
        assertTrue(result.contains("FileStatus"));
    }

    /**
     * Test of PutQuery method, of class JerseyJsonMethod.
     */
    @Test
    public void testPutQuery() {
        System.out.println("PutQuery");
        
        // Test creation of empty file on HDFS.
        String url = "http://localhost:50070/webhdfs/v1/user/michaeljones/empty.txt";
        List<Pair<String, String>> queryParams = new ArrayList();
        queryParams.add(new Pair<>("user.name","michaeljones"));
        queryParams.add(new Pair<>("op","CREATE"));
        queryParams.add(new Pair<>("op","CREATE"));
        queryParams.add(new Pair<>("overwrite","true"));
        
        JerseyJsonMethod instance = new JerseyJsonMethod();
        int expCreatedResult = 201;
        StringBuilder redirectLocation = new StringBuilder();
        int result = instance.PutQuery(url, queryParams, redirectLocation);
        assertEquals(expCreatedResult, result);
        assertTrue(redirectLocation.toString().length() == 0);
    }

    /**
     * Test of PutFile method, of class JerseyJsonMethod.
     * @throws java.lang.Exception
     */
    @Test
    public void testPutFile() throws Exception {
        System.out.println("PutFile");
        
        // Test copy of local file to HDFS.
        String url = "http://localhost:50070/webhdfs/v1/user/michaeljones/hello.log";
        List<Pair<String, String>> queryParams = new ArrayList();
        queryParams.add(new Pair<>("user.name","michaeljones"));
        queryParams.add(new Pair<>("op","CREATE"));
        queryParams.add(new Pair<>("op","CREATE"));
        queryParams.add(new Pair<>("overwrite","true"));
        
        JerseyJsonMethod instance = new JerseyJsonMethod();
        int expCreatedResult = 201;
        String localFilePath= "hello.log";
        int result = instance.PutFile(url, localFilePath, queryParams);
        assertEquals(expCreatedResult, result);
    }
    
}
