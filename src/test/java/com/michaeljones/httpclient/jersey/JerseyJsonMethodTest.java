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
public class JerseyJsonMethodTest {
        // Send the logs to the same appender as for the hellohadoopworld package.
    private static final Logger LOGGER = LoggerFactory.getLogger(HelloHdfsTest.class.getName());

    
    public JerseyJsonMethodTest() {
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
     * Test of GetStringContent method, of class JerseyJsonMethod.
     */
    @Test
    public void testGetStringContent() {
        System.out.println("GetStringContent");
        
        // HDFS needs to be running, because the following URI is a Web HDFS resource.
        String url = "http://localhost:50070/webhdfs/v1/user/michaeljones/?op=LISTSTATUS";
        JerseyJsonMethod instance = new JerseyJsonMethod();
        String result = instance.GetStringContent(url);
        
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
        int result = instance.PutQuery(url, queryParams);
        assertEquals(expCreatedResult, result);
    }

    /**
     * Test of PutFile method, of class JerseyJsonMethod.
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