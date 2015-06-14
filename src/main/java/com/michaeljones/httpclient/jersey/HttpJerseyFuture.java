/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.michaeljones.httpclient.jersey;

import com.michaeljones.httpclient.HttpMethodFuture;
import com.sun.jersey.api.client.ClientResponse;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.logging.Level;
import javax.ws.rs.core.MultivaluedMap;
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
            // Note the following makes this a retrieve once only. Which is ok for this demo
            // program which gets either the status or the redirect location. If business
            // use case required, we could cache all results before closing.
            try {
                int status = jerseyFuture.get().getStatus();
                return status;
            }
            finally {
                jerseyFuture.get().close();
            }
        } catch (InterruptedException | ExecutionException ex) {
            LOGGER.error("Jersey Future: " + ex.getMessage());
            throw new RuntimeException(ex.getMessage());
        }
    }    

    @Override
    public String GetRedirectLocation() {
        try {
            ClientResponse response = jerseyFuture.get();
            MultivaluedMap<String, String> headers = response.getHeaders();

            // Note the following makes this a retrieve once only. Which is ok for this demo
            // program which gets either the status or the redirect location. If business
            // use case required, we could cache all results before closing.
            try {
                List<String> hdrs = headers.get("Location");
                if (hdrs.size() > 0) {
                    String redirectLocation = hdrs.get(0);

                    LOGGER.debug("Jersey client Redirect to: " + redirectLocation);
                    return redirectLocation;
                }

            } finally {
                // I believe this releases the connection to the client pool, but does not
                // close the connection.
                response.close();
            }
        } catch (InterruptedException | ExecutionException ex) {
            LOGGER.error("Jersey Future: " + ex.getMessage());
            throw new RuntimeException(ex.getMessage());
        }

        throw new RuntimeException("Future had no redirect location");
    }
}
