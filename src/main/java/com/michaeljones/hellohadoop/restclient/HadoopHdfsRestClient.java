/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.michaeljones.hellohadoop.restclient;

import com.michaeljones.httpclient.HttpMethodClient;
import com.michaeljones.httpclient.HttpMethodFuture;
import com.michaeljones.httpclient.apache.ApacheJsonMethod;
import com.michaeljones.httpclient.jersey.JerseyMethodClient;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.math3.util.Pair;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.slf4j.LoggerFactory;

/**
 *
 * @author michaeljones
 */
public class HadoopHdfsRestClient {
    
    // %1 nameNodeHost, %2 username %3 resource.
    private static final String  BASIC_URL_FORMAT = "http://%1$s:50070/webhdfs/v1/user/%2$s/%3$s";
    
    private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(ApacheJsonMethod.class.getName());

    private HttpMethodClient restImpl;
    private final String nameNodeHost;
    private final String username;

    private HadoopHdfsRestClient(String host, String username) {
        this.nameNodeHost = host;
        this.username = username;
    }

    // The factory method allows us to create different underlying implementations of this client.
    public static HadoopHdfsRestClient JerseyClientFactory(String host, String username) {
        HadoopHdfsRestClient client = new HadoopHdfsRestClient(host, username);
        client.restImpl = new JerseyMethodClient();

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
            // %1 nameNodeHost, %2 username %3 resource.
            String uri = String.format(BASIC_URL_FORMAT, nameNodeHost, username, remoteRelativePath);
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
            LOGGER.error("Hadoop List directory failed: " + ex.getMessage());
            throw new RuntimeException("Hadoop List directory failed :" + ex.getMessage());
        }
        finally {
            restImpl.Close();
        }
    }
    
    public void CreateEmptyFile(String remoteRelativePath) {
        // %1 nameNodeHost, %2 username %3 resource.
        String uri = String.format(BASIC_URL_FORMAT, nameNodeHost, username, remoteRelativePath);
        List<Pair<String, String>> queryParams = new ArrayList();
        queryParams.add(new Pair<>("user.name","michaeljones"));
        queryParams.add(new Pair<>("op","CREATE"));
        queryParams.add(new Pair<>("overwrite","true"));
        
        try {
            StringBuilder redirectLocation = new StringBuilder();

            int httpCode = restImpl.PutQuery(uri, queryParams, redirectLocation);
            
            // NB two separate PUTs may be needed which is not strictly REST, but
            // this is the Hadoop documented procedure.
            switch (httpCode) {
                case 307:
                    // The above PUT to the Hadoop name node has returned us a redirection
                    // to the Hadoop data node.
                    String dataNodeURI = redirectLocation.toString();
                    if (dataNodeURI.length() == 0) {
                        throw new RuntimeException("Create file redirect error");
                    }
                    
                    httpCode = restImpl.PutQuery(dataNodeURI, null, null);
                    break;
                    
                case 201:
                    // HTTP backends which correctly implement “Expect: 100-continue”
                    // Will return 201 created immediately.
                    break;
                    
                default:
                    throw new RuntimeException("Create File failed : HTTP error code : " + httpCode);
            }

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
        // %1 nameNodeHost, %2 username %3 resource.
        String uri = String.format(BASIC_URL_FORMAT, nameNodeHost, username, remoteRelativePath);
        List<Pair<String, String>> queryParams = new ArrayList();
        queryParams.add(new Pair<>("user.name","michaeljones"));
        queryParams.add(new Pair<>("op","CREATE"));
        queryParams.add(new Pair<>("overwrite","true"));

        try {
            StringBuilder redirectLocation = new StringBuilder();
            int httpCode = restImpl.PutFile(uri, localPath, queryParams, redirectLocation);
            
            // NB two separate PUTs may be needed which is not strictly REST, but
            // this is the Hadoop documented procedure.
            switch (httpCode) {
                case 307:
                    // The above PUT to the Hadoop name node has returned us a redirection
                    // to the Hadoop data node.
                    String dataNodeURI = redirectLocation.toString();
                    if (dataNodeURI.length() == 0) {
                        LOGGER.error("Hadoop redirect location empty");
                        throw new RuntimeException("Create file redirect error");
                    }
                    
                    httpCode = restImpl.PutFile(dataNodeURI, localPath, null, null);
                    break;
                    
                case 201:
                    // HTTP backends which correctly implement “Expect: 100-continue”
                    // Will return 201 created immediately.
                    break;
                    
                default:
                    throw new RuntimeException("Create File failed : HTTP error code : " + httpCode);
            }

            if (httpCode != 201) {
                throw new RuntimeException("Create File failed : HTTP error code : " + httpCode);
            }
        } catch (FileNotFoundException ex) {
            LOGGER.error("Hadoop upload file not found: " + ex.getMessage());
            throw new RuntimeException("Create File failed : " + ex.getMessage());
        }
        finally {
            // We want to close TCP connections immediately, because garbage collection time
            // is non-deterministic.
            restImpl.Close();
        }        
    }
    
    public void ParallelUpload(List<Pair<String, String>> remoteLocalPairs) {
        // First of all do a concurrent gathering of the redirect locations from the name node.
        // Does this help if all calls are going to the same name node?
        // Answer: a little, because we are performing multiple round trips concurrently. Packets
        // are interleaved, but we are not individually blocking on each round trip interval.
        List<HttpMethodFuture> redirectFutures = new ArrayList();
        for (Pair<String, String> remoteLocal : remoteLocalPairs) {
            HttpMethodFuture redirect = GetRedirectLocationAsync(remoteLocal.getFirst(), remoteLocal.getSecond());
            redirectFutures.add(redirect);
        }

        // Now wait for all the redirect locations to arrive, by getting the result from the futures.
        // Make a new list of redirect location, local path pairs.
        // This will take as long as the longest redirect retrieval.
        List<Pair<String, String>> redirectLocalPairs = new ArrayList();
        for (HttpMethodFuture redirect : redirectFutures) {
            // This may block depending on whether any of the preceding futures took longer or not.
            String redirectLocation = redirect.GetRedirectLocation();
            String localPath = remoteLocalPairs.get(0).getSecond();
            redirectLocalPairs.add(new Pair<>(redirectLocation, localPath));
        }
        
        List<HttpMethodFuture> uploadFutures = new ArrayList();

        // The final step is to perform a concurrent/parallel upload to the data nodes.
        // In a multi-homed cluster these could be going out on separate network interfaces.
        // The bottleneck would then be the file storage. If some source paths were coming from
        // different file servers then we would really win with this strategy.
        for (Pair<String, String> redirectLocal : redirectLocalPairs) {
            HttpMethodFuture future = UploadFileAsync(redirectLocal.getFirst(), redirectLocal.getSecond());
            uploadFutures.add(future);
        }
        
        // Now wait for all uploads to complete, by getting the result from each future.
        // This will take as long as the longest upload.
        for (HttpMethodFuture upload : uploadFutures) {
            // This may block depending on whether any previous futures took longer or not.
            int httpStatusCode = upload.GetHttpStatusCode();
            if (httpStatusCode != 201) {
                // Should really log which file.
                LOGGER.error("Hadoop parallel upload unexpected status: " + httpStatusCode);                
            }
        }
    }

    public HttpMethodFuture GetRedirectLocationAsync(String remoteRelativePath, String localPath) {
        // %1 nameNodeHost, %2 username %3 resource.
        String uri = String.format(BASIC_URL_FORMAT, nameNodeHost, username, remoteRelativePath);
        List<Pair<String, String>> queryParams = new ArrayList();
        queryParams.add(new Pair<>("user.name","michaeljones"));
        queryParams.add(new Pair<>("op","CREATE"));
        queryParams.add(new Pair<>("overwrite","true"));
        
        try {        
            return restImpl.GetRedirectLocationAsync(uri, localPath, queryParams);
        } catch (FileNotFoundException ex) {
            LOGGER.error("Hadoop get redirect location async file not found: " + ex.getMessage());
            throw new RuntimeException("Create File failed : " + ex.getMessage());
        }
    }
    
    public HttpMethodFuture UploadFileAsync(String redirectLocation, String localPath) {

        try {
            return restImpl.PutFileAsync(redirectLocation, localPath);
        } catch (FileNotFoundException ex) {
            LOGGER.error("Hadoop async upload file not found: " + ex.getMessage());
            throw new RuntimeException("Create File failed : " + ex.getMessage());
        }
        finally {
            // We want to close TCP connections immediately, because garbage collection time
            // is non-deterministic.
            restImpl.Close();
        }        
    }
    
    public void SetBigChunkSize() {
        restImpl.SetBigChunkSize();
    }
}
