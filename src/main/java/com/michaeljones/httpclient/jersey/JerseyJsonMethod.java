/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.michaeljones.httpclient.jersey;

import com.michaeljones.httpclient.HttpJsonMethod;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.util.List;
import javax.ws.rs.core.MediaType;
import org.apache.commons.math3.util.Pair;


/**
 *
 * @author michaeljones
 */
public class JerseyJsonMethod implements HttpJsonMethod {
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
    public int PutQuery(String url, List<Pair<String, String>> queryParams) {
        WebResource putResource = jerseyImpl.resource(url);
        for (Pair<String, String> queryParam : queryParams) {
            putResource = putResource.queryParam(queryParam.getFirst(), queryParam.getSecond());
        }
        
        ClientResponse putResponse = putResource.put(ClientResponse.class);
        
        return putResponse.getStatus();
    }
    
    @Override
    public int PutFile(String url, String filePath, List<Pair<String, String>> queryParams) throws FileNotFoundException {
        WebResource fileResource = jerseyImpl.resource(url);
        for (Pair<String, String> queryParam : queryParams) {
            fileResource = fileResource.queryParam(queryParam.getFirst(), queryParam.getSecond());
        }

        InputStream  fileInStream = new FileInputStream(filePath);
        ClientResponse response = fileResource.type(MediaType.APPLICATION_OCTET_STREAM).put(ClientResponse.class, fileInStream);
         
        return response.getStatus();
    }

    @Override
    public void Close() {
        jerseyImpl.destroy();
    }
}
