/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.michaeljones.httpclient.jersey;

import com.michaeljones.httpclient.HttpJsonMethod;
import com.michaeljones.httpclient.HttpMethodFuture;
import com.michaeljones.httpclient.apache.ApacheJsonMethod;
import com.sun.jersey.api.client.AsyncWebResource;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
import static com.sun.jersey.api.client.config.ClientConfig.PROPERTY_CHUNKED_ENCODING_SIZE;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import org.apache.commons.math3.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author michaeljones
 */
public class JerseyJsonMethod implements HttpJsonMethod {

    private static final Logger LOGGER = LoggerFactory.getLogger(ApacheJsonMethod.class.getName());
    private static final int CHUNK_SIZE = 1024 * 1024;

    Client jerseyImpl;

    public JerseyJsonMethod() {
        jerseyImpl = Client.create();
    }

    @Override
    public String GetStringContent(String url, List<Pair<String, String>> queryParams) {
        WebResource getResource = jerseyImpl.resource(url);
        for (Pair<String, String> queryParam : queryParams) {
            getResource = getResource.queryParam(queryParam.getFirst(), queryParam.getSecond());
        }

        ClientResponse response = getResource.accept("application/json").get(ClientResponse.class);
        if (response.getStatus() != 200) {
            throw new RuntimeException("Jersey client failed : HTTP error code : " + response.getStatus());
        }

        String result = response.getEntity(String.class);

        return result;
    }

    @Override
    public int PutQuery(String url, List<Pair<String, String>> queryParams, StringBuilder redirect) {
        WebResource putResource = jerseyImpl.resource(url);

        if (queryParams != null) {
            for (Pair<String, String> queryParam : queryParams) {
                putResource = putResource.queryParam(queryParam.getFirst(), queryParam.getSecond());
            }
        }

        ClientResponse putResponse = putResource.put(ClientResponse.class);

        // This implementation does not append to the redirect parameter.
        return putResponse.getStatus();
    }

    @Override
    public int PutFile(
            String url,
            String filePath,
            List<Pair<String, String>> queryParams,
            StringBuilder redirect) throws FileNotFoundException {
        WebResource fileResource = jerseyImpl.resource(url);
        
        if (queryParams != null) {
            // Query params are optional. In the case of a redirect the url will contain
            // all the params.
            for (Pair<String, String> queryParam : queryParams) {
                fileResource = fileResource.queryParam(queryParam.getFirst(), queryParam.getSecond());
            }
        }

        InputStream fileInStream = new FileInputStream(filePath);
        ClientResponse response = fileResource.type(MediaType.APPLICATION_OCTET_STREAM).put(ClientResponse.class, fileInStream);

        // This implementation does not append to the redirect parameter.
        MultivaluedMap<String, String> headers = response.getHeaders();
        try {
            List<String> hdrs = headers.get("Location");
            if (redirect != null && hdrs.size() > 0) {
                String redirectLocation = hdrs.get(0);

                redirect.append(redirectLocation);
                LOGGER.debug("Jersey client Redirect to: " + redirectLocation);
            }

        } finally {
                // I believe this releases the connection to the client pool, but does not
            // close the connection.
            response.close();
        }

        return response.getStatus();
    }

    public int PutFileAlternative(
            String url,
            String filePath,
            List<Pair<String, String>> queryParams,
            StringBuilder redirect) throws FileNotFoundException {
        WebResource fileResource = jerseyImpl.resource(url);
        for (Pair<String, String> queryParam : queryParams) {
            fileResource = fileResource.queryParam(queryParam.getFirst(), queryParam.getSecond());
        }

        InputStream fileInStream = new FileInputStream(filePath);

        // Alternative - I think this is easier to conceptualise.
        WebResource.Builder request = fileResource.getRequestBuilder();
        request = request.entity(fileInStream, MediaType.APPLICATION_OCTET_STREAM);

        ClientResponse response = request.put(ClientResponse.class);

        // This implementation does not append to the redirect parameter.
        return response.getStatus();
    }
    
    public HttpMethodFuture PutFileAsync(String redirectUrl, String filePath) throws FileNotFoundException {
        // NB this method has to be preceded by a put of the file URI returning a redirection.
        // This is not strictly REST, but it is the way Hadoop works.
        AsyncWebResource fileResource = jerseyImpl.asyncResource(redirectUrl);
        InputStream fileInStream = new FileInputStream(filePath);

        AsyncWebResource.Builder request = fileResource.getRequestBuilder();
        request = request.entity(fileInStream, MediaType.APPLICATION_OCTET_STREAM);
        
        Future<ClientResponse> future = request.put(ClientResponse.class);
        
        return new HttpJerseyFuture(future);
    }

    @Override
    public void Close() {
        jerseyImpl.destroy();
    }

    @Override
    public void SetBigChunkSize() {
        jerseyImpl.setChunkedEncodingSize(CHUNK_SIZE);
        Map<String, Object> properties = jerseyImpl.getProperties();
        Object retChunkProperty = properties.get(PROPERTY_CHUNKED_ENCODING_SIZE);
        assert (retChunkProperty instanceof java.lang.Integer);
        if ((Integer) retChunkProperty != CHUNK_SIZE) {
            LOGGER.warn("Requested Chunk size: " + CHUNK_SIZE + " returned property: " + retChunkProperty);
        } else {
            LOGGER.info(PROPERTY_CHUNKED_ENCODING_SIZE + ": " + retChunkProperty);
        }
    }
}
