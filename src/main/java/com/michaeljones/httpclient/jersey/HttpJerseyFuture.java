/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.michaeljones.httpclient.jersey;

import java.io.IOException;
import com.michaeljones.httpclient.HttpMethodFuture;
import com.sun.jersey.api.client.ClientResponse;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author michaeljones
 */
public class HttpJerseyFuture implements HttpMethodFuture {
    private static final Logger LOGGER = LoggerFactory.getLogger(HttpJerseyFuture.class.getName());
    private final Future<ClientResponse> jerseyFuture;
    
    public HttpJerseyFuture(Future<ClientResponse> future) {
        jerseyFuture = future;
    }

    @Override
    public int GetHttpStatusCode() {
        try {
            return jerseyFuture.get().getStatus();
        } catch (InterruptedException | ExecutionException ex) {
            LOGGER.error("Jersey Future: " + ex.getMessage());
            throw new RuntimeException(ex.getMessage());
        }
    }    
}
