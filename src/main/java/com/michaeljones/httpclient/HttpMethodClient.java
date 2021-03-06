/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.michaeljones.httpclient;

import java.io.FileNotFoundException;
import java.util.List;
import org.apache.commons.math3.util.Pair;

/**
 * Provide a common client interface to either a jersey or apache client back end.
 * @author michaeljones
 */
public interface HttpMethodClient {

    String GetStringContent(String url, List<Pair<String, String>> queryParams);

    int PutQuery(String url, List<Pair<String, String>> queryParams, StringBuilder redirectLocation);

    int PutFile(
            String url,
            String filePath,
            List<Pair<String, String>> queryParams,
            StringBuilder redirectLocation) throws FileNotFoundException;

    HttpMethodFuture GetRedirectLocationAsync(
            String url,
            String filePath,
            List<Pair<String, String>> queryParams) throws FileNotFoundException;

    HttpMethodFuture PutFileAsync(String redirectUrl, String filePath) throws FileNotFoundException;

    void SetBigChunkSize();
    
    int DeleteFile(String url, List<Pair<String, String>> queryParams);

    void Close();

}
