/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.michaeljones.httpclient.apache;

import com.michaeljones.hellohadoopworldmaven.HelloHdfsTest;
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
public class ApacheJsonMethodTest {
        // Send the logs to the same appender as for the hellohadoopworld package.
    private static final Logger LOGGER = LoggerFactory.getLogger(HelloHdfsTest.class.getName());
    
    public ApacheJsonMethodTest() {
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
     * Test of GetStringContent method, of class ApacheJsonMethod.
     */
    @Test
    public void testGetStringContent() {
        System.out.println("GetStringContent");
        // HDFS needs to be running, because the following URI is a Web HDFS resource.
        String url = "http://localhost:50070/webhdfs/v1/user/michaeljones/?op=LISTSTATUS";

        ApacheJsonMethod instance = new ApacheJsonMethod();

        String result = instance.GetStringContent(url);
        
        LOGGER.info("Apache client testGetStringContent");
        LOGGER.info(result);

        assertTrue(result.contains("FileStatus"));
    }
    
}
