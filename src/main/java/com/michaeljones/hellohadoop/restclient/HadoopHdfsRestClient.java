/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.michaeljones.hellohadoop.restclient;

import com.michaeljones.httpclient.HttpJsonMethod;
import com.michaeljones.httpclient.apache.ApacheJsonMethod;
import com.michaeljones.httpclient.jersey.JerseyJsonMethod;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.math3.util.Pair;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

/**
 *
 * @author michaeljones
 */
public class HadoopHdfsRestClient {
    
    // %1 host, %2 username %3 resource.
    private static final String  BASIC_URL_FORMAT = "http://%1$s:50070/webhdfs/v1/user/%2$s/%3$s";

    private HttpJsonMethod restImpl;
    private String host;
    private String username;

    private HadoopHdfsRestClient(String host, String username) {
        this.host = host;
        this.username = username;
    }

    // The factory method allows us to create different underlying implementations of this client.
    public static HadoopHdfsRestClient JerseyClientFactory(String host, String username) {
        HadoopHdfsRestClient client = new HadoopHdfsRestClient(host, username);
        client.restImpl = new JerseyJsonMethod();

        return client;
    }

    // The factory method allows us to create different underlying implementations of this client.
    public static HadoopHdfsRestClient ApacheClientFactory(String host, String username) {
        HadoopHdfsRestClient client = new HadoopHdfsRestClient(host, username);
        client.restImpl = new ApacheJsonMethod();

        return client;
    }

    public String[] ListDirectorySimple(String remoteRelativePath) {
        try {
            // %1 host, %2 username %3 resource.
            String uri = String.format(BASIC_URL_FORMAT, host, username, remoteRelativePath);
            List<Pair<String, String>> queryParams = new ArrayList();
            queryParams.add(new Pair<>("user.name","michaeljones"));
            queryParams.add(new Pair<>("op","LISTSTATUS"));
            
            String content = restImpl.GetStringContent(uri, queryParams);
            
            JSONParser parser = new JSONParser();
            JSONObject jsonObject = (JSONObject) parser.parse(content);
            
            JSONObject listStatus = (JSONObject) jsonObject.get("FileStatuses");
            JSONArray fileList = (JSONArray) listStatus.get("FileStatus");
            String[] directoryListing = new String[fileList.size()];
            int directoryIndex = 0;
            for (Object listing : fileList) {
                JSONObject jsonListing = (JSONObject) listing;
                String pathname = jsonListing.get("pathSuffix").toString();
                directoryListing[directoryIndex++] = pathname;
            }
            
            return directoryListing;
        } catch (ParseException ex) {
            // FIX ME logger to sl4j.
            Logger.getLogger(HadoopHdfsRestClient.class.getName()).log(Level.SEVERE, null, ex);
            throw new RuntimeException("List directory failed :" + ex.getMessage());
        }
        finally {
            restImpl.Close();
        }
    }
    
    public void CreateEmptyFile(String remoteRelativePath) {
        // %1 host, %2 username %3 resource.
        String uri = String.format(BASIC_URL_FORMAT, host, username, remoteRelativePath);
        List<Pair<String, String>> queryParams = new ArrayList();
        queryParams.add(new Pair<>("user.name","michaeljones"));
        queryParams.add(new Pair<>("op","CREATE"));
        queryParams.add(new Pair<>("overwrite","true"));
        
        try {
            int httpCode = restImpl.PutQuery(uri, queryParams, null);

            if (httpCode != 201) {
                throw new RuntimeException("Create File failed : HTTP error code : " + httpCode);
            }
        }
        finally {
            // We want to close TCP connections immediately, because garbage collection time
            // is non-deterministic.
            restImpl.Close();
        }
    }
    
    public void UploadFile(String remoteRelativePath, String localPath) {
        // %1 host, %2 username %3 resource.
        String uri = String.format(BASIC_URL_FORMAT, host, username, remoteRelativePath);
        List<Pair<String, String>> queryParams = new ArrayList();
        queryParams.add(new Pair<>("user.name","michaeljones"));
        queryParams.add(new Pair<>("op","CREATE"));
        queryParams.add(new Pair<>("overwrite","true"));

        try {
            int httpCode = restImpl.PutFile(uri, localPath, queryParams);
            if (httpCode != 201) {
                throw new RuntimeException("Create File failed : HTTP error code : " + httpCode);
            }
        } catch (FileNotFoundException ex) {
            // FIX ME logger to sl4j.
            Logger.getLogger(HadoopHdfsRestClient.class.getName()).log(Level.SEVERE, null, ex);
            throw new RuntimeException("Create File failed : " + ex.getMessage());
        }
        finally {
            // We want to close TCP connections immediately, because garbage collection time
            // is non-deterministic.
            restImpl.Close();
        }        
    }
}
