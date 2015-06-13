/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.michaeljones.httpclient.jersey;

import com.michaeljones.hellohadoopworldmaven.HelloHdfsTest;
import com.michaeljones.httpclient.HttpMethodFuture;
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
        queryParams.add(new Pair<>("overwrite","true"));
        
        JerseyJsonMethod instance = new JerseyJsonMethod();
        int expCreatedResult = 201;
        String localFilePath= "hello.log";
        StringBuilder redirectLocation = new StringBuilder();
        int result = instance.PutFile(url, localFilePath, queryParams, redirectLocation);
        assertEquals(expCreatedResult, result);
        
        // I am not sure why we get a redirect location on the IPC 9000 port, but we do.
        // NB the http status code still returns created.
        assertTrue(redirectLocation.indexOf(":9000") > 0);
        LOGGER.info("Jersey redirected to IPC: " + redirectLocation);
    }

    /**
     * Test of PutFileAlternative method, of class JerseyJsonMethod.
     */
    @Test
    public void testPutFileAlternative() throws Exception {
        System.out.println("PutFileAlternative");
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
        StringBuilder redirectLocation = new StringBuilder();
        int result = instance.PutFileAlternative(url, localFilePath, queryParams, redirectLocation);
        assertEquals(expCreatedResult, result);
        assertTrue(redirectLocation.length() == 0);
    }

    /**
     * Test of Close method, of class JerseyJsonMethod.
     */
    @Test
    public void testClose() {
        System.out.println("Close");
        JerseyJsonMethod instance = new JerseyJsonMethod();
        instance.Close();
        
        // Not a lot to test.
        assertTrue(true);
    }

    /**
     * Test of SetBigChunkSize method, of class JerseyJsonMethod.
     */
    @Test
    public void testSetBigChunkSize() {
        System.out.println("SetBigChunkSize");
        JerseyJsonMethod instance = new JerseyJsonMethod();
        instance.SetBigChunkSize();
        
        // Not a lot to test.
        assertTrue(true);
    }

    /**
     * Test of PutFileAsync method, of class JerseyJsonMethod.
     * @throws java.lang.Exception
     */
    @Test
    public void testPutFileAsync() throws Exception {
        // First of all get the redirect.
        String url = "http://localhost:50070/webhdfs/v1/user/michaeljones/nbactions.xml";
        List<Pair<String, String>> queryParams = new ArrayList();
        queryParams.add(new Pair<>("user.name","michaeljones"));
        queryParams.add(new Pair<>("op","CREATE"));
        queryParams.add(new Pair<>("overwrite","true"));
        
        JerseyJsonMethod instance = new JerseyJsonMethod();
        
        // This is important - without this the Jersey client does not redirect.
        instance.SetBigChunkSize();

        // This bit is not async. However, it should be a quick round trip to the name node
        // to return us the redirect URL on which we perform the async method.
        int expRedirectResult = 307;
        StringBuilder redirectLocation = new StringBuilder();
        String localFilePath = "nbactions.xml";
        int result = instance.PutFile(url, localFilePath, queryParams, redirectLocation);
        assertEquals(expRedirectResult, result);
        assertTrue(redirectLocation.toString().length() > 0);

        System.out.println("PutFileAsync");
        
        // If we got this far then a redirect url was returned and we do the async upload to this.
        // This is the time consuming part and the where we stand to gain from the async.
        int expCreatedResult = 201;
        HttpMethodFuture future = instance.PutFileAsync(redirectLocation.toString(), localFilePath);
        assertEquals(expCreatedResult, future.GetHttpStatusCode());
    }
    
}
