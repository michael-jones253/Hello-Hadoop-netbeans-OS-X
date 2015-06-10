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
    public String GetStringContent(String url) {
        WebResource resource = jerseyImpl.resource(url);
        ClientResponse response = resource.accept("application/json").get(ClientResponse.class);
        if (response.getStatus() != 200) {
            throw new RuntimeException("Jersey client failed : HTTP error code : " + response.getStatus());
        }
        
        String result = response.getEntity(String.class);
        
        return result;
    }   
}
