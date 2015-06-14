/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.michaeljones.httpclient.apache;

import com.michaeljones.httpclient.HttpJsonMethod;
import com.michaeljones.httpclient.HttpMethodFuture;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import org.apache.commons.math3.util.Pair;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

import java.net.URISyntaxException;
import org.apache.http.Header;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.InputStreamEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author michaeljones
 */
public class ApacheJsonMethod implements HttpJsonMethod {

    private static final Logger LOGGER = LoggerFactory.getLogger(ApacheJsonMethod.class.getName());

    CloseableHttpClient clientImpl;

    public ApacheJsonMethod() {
        clientImpl = HttpClients.createDefault();
    }

    @Override
    public String GetStringContent(String url, List<Pair<String, String>> queryParams) {
        try {
            HttpGet httpget = new HttpGet(url);

            // The Hadoop Web HDFS only serves json, but no harm in being explicit.
            httpget.setHeader("accept", "application/json");

            URIBuilder builder = new URIBuilder(url);

            for (Pair<String, String> queryParam : queryParams) {
                builder.addParameter(queryParam.getFirst(), queryParam.getSecond());
            }

            httpget.setURI(builder.build());

            CloseableHttpResponse response = clientImpl.execute(httpget);
            try {
                HttpEntity entity = response.getEntity();
                return EntityUtils.toString(entity);
            } finally {
                // I believe this releases the connection to the client pool, but does not
                // close the connection.
                response.close();
            }
        } catch (IOException | URISyntaxException ex) {
            throw new RuntimeException("Apache method get string content: " + ex.getMessage());
        }
    }

    @Override
    public int PutQuery(String url, List<Pair<String, String>> queryParams, StringBuilder redirect) {
        try {
            HttpPut httpPut = new HttpPut();
            URIBuilder fileUri = new URIBuilder(url);

            if (queryParams != null) {
                for (Pair<String, String> queryParam : queryParams) {
                    fileUri.addParameter(queryParam.getFirst(), queryParam.getSecond());
                }
            }

            httpPut = new HttpPut(fileUri.build());
            CloseableHttpResponse response = clientImpl.execute(httpPut);
            try {
                Header[] hdrs = response.getHeaders("Location");
                if (redirect != null && hdrs.length > 0) {
                    String redirectLocation = hdrs[0].getValue();

                    redirect.append(redirectLocation);
                    LOGGER.debug("Redirect to: " + redirectLocation);
                }

                return response.getStatusLine().getStatusCode();
            } finally {
                // I believe this releases the connection to the client pool, but does not
                // close the connection.
                response.close();
            }
        } catch (IOException | URISyntaxException ex) {
            throw new RuntimeException("Apache method putQuery: " + ex.getMessage());
        }
    }

    @Override
    public int PutFile(
               String url,
               String filePath,
               List<Pair<String, String>> queryParams,
               StringBuilder redirect) throws FileNotFoundException {
        try {
            HttpPut httpPut = new HttpPut();
            URIBuilder fileUri = new URIBuilder(url);

            if (queryParams != null) {
                // Query params are optional. In the case of a redirect the url will contain
                // all the params.
                for (Pair<String, String> queryParam : queryParams) {
                    fileUri.addParameter(queryParam.getFirst(), queryParam.getSecond());
                }
            }

            httpPut = new HttpPut(fileUri.build());
            InputStream fileInStream = new FileInputStream(filePath);
            InputStreamEntity chunkedStream = new InputStreamEntity(
                    fileInStream, -1, ContentType.APPLICATION_OCTET_STREAM);
            chunkedStream.setChunked(true);
            
            httpPut.setEntity(chunkedStream);

            CloseableHttpResponse response = clientImpl.execute(httpPut);
            try {
                Header[] hdrs = response.getHeaders("Location");
                if (redirect != null && hdrs.length > 0) {
                    String redirectLocation = hdrs[0].getValue();

                    redirect.append(redirectLocation);
                    LOGGER.debug("Redirect to: " + redirectLocation);
                }

                return response.getStatusLine().getStatusCode();
            } finally {
                // I believe this releases the connection to the client pool, but does not
                // close the connection.
                response.close();
            }
        } catch (IOException | URISyntaxException ex) {
            throw new RuntimeException("Apache method putQuery: " + ex.getMessage());
        }
    }

    @Override
    public void Close() {
        try {
            // I believe this closes all connections held by the client.
            clientImpl.close();
        } catch (IOException ex) {
            throw new RuntimeException("Jersey client close : " + ex.getMessage());
        }
    }

    @Override
    public void SetBigChunkSize() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public HttpMethodFuture PutFileAsync(String redirectUrl, String filePath) throws FileNotFoundException {
        
        // Please see the JerseyJsonMethod implementation for how this is intended to work.
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public HttpMethodFuture GetRedirectLocationAsync(String url, String filePath, List<Pair<String, String>> queryParams) throws FileNotFoundException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

}
