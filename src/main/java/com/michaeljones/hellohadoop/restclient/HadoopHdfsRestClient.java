/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.michaeljones.hellohadoop.restclient;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import com.michaeljones.httpclient.HttpJsonMethod;
import com.michaeljones.httpclient.jersey.JerseyJsonMethod;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.json.simple.JSONArray;
import org.json.simple.parser.ParseException;

/**
 *
 * @author michaeljones
 */
public class HadoopHdfsRestClient {

    private HttpJsonMethod restImpl;
    private String host;
    private String username;

    private HadoopHdfsRestClient(String host, String username) {
        this.host = host;
        this.username = username;
    }

    public static HadoopHdfsRestClient JerseyClientFactory(String host, String username) {
        HadoopHdfsRestClient client = new HadoopHdfsRestClient(host, username);
        client.restImpl = new JerseyJsonMethod();

        return client;
    }

    public String[] ListDirectorySimple(String relativePath) {
        try {
            String urlFormat = "http://%1$s:50070/webhdfs/v1/user/%2$s/%3$s/?op=LISTSTATUS";
            
            String format = String.format(urlFormat, host, username, relativePath);
            String content = restImpl.GetStringContent(format);
            
            // FIX ME json parsing.
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
            Logger.getLogger(HadoopHdfsRestClient.class.getName()).log(Level.SEVERE, null, ex);
        }
        
        return null;
    }
}
