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
 *
 * @author michaeljones
 */
public interface HttpJsonMethod {
    String GetStringContent(String url, List<Pair<String, String>> queryParams); 
    
    int PutQuery(String url, List<Pair<String, String>> queryParams);
    
    int PutFile(String url, String filePath, List<Pair<String, String>> queryParams) throws FileNotFoundException;
    
    void Close();

}
