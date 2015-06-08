/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.michaeljones.hellohadoopworldmaven;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author michaeljones
 */
public class HelloHadoopWorld {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        try {
            Logger.getLogger(HelloHadoopWorld.class.getName()).log(Level.INFO, "Hello Hadoop World!");
            
            HelloHdfs hdfs = new HelloHdfs();
            hdfs.writeFile();
        } catch (IOException ex) {
            Logger.getLogger(HelloHadoopWorld.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
    
}
