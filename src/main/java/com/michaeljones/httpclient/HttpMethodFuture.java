/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.michaeljones.httpclient;

/**
 * Provide a common HTTP method future result interface to jersey or apache implementations.
 * @author michaeljones
 */
public interface HttpMethodFuture {
    int GetHttpStatusCode();
    
    String GetRedirectLocation();
}
