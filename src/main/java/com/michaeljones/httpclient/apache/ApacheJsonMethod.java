/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.michaeljones.httpclient.apache;

import com.michaeljones.httpclient.HttpJsonMethod;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.util.EntityUtils;

/**
 *
 * @author michaeljones
 */
public class ApacheJsonMethod implements HttpJsonMethod {
    CloseableHttpClient clientImpl;
    
    public ApacheJsonMethod() {
        clientImpl = HttpClients.createDefault();
    }

    @Override
    public String GetStringContent(String url) {
        String content = null;
        try {
            HttpGet httpget = new HttpGet(url);
            
            // The Hadoop Web HDFS only serves json, but no harm in being explicit.
            httpget.setHeader("accept", "application/json");            
            
            CloseableHttpResponse response = clientImpl.execute(httpget);
            HttpEntity entity = response.getEntity();
            content = EntityUtils.toString(entity);
        } catch (IOException ex) {
            Logger.getLogger(ApacheJsonMethod.class.getName()).log(Level.SEVERE, null, ex);
        }
        finally {
            try {
                clientImpl.close();
            } catch (IOException ex) {
                Logger.getLogger(ApacheJsonMethod.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
        
        return content;
    }
    
}
