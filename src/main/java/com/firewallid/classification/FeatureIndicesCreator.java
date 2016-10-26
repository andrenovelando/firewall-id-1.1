/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.firewallid.classification;

import com.firewallid.util.FIConfiguration;
import java.io.IOException;
import org.apache.spark.api.java.JavaSparkContext;

/**
 *
 * @author andrenovelando@gmail.com
 */
public class FeatureIndicesCreator {

    public static void main(String[] args) throws IOException {
        /*Inisiasi JavaSparkContext*/
        JavaSparkContext jsc = new JavaSparkContext(FIConfiguration.createSparkConf(null));

        /* Create feature indice file */
        new FeatureExtraction(jsc).saveFeatureIndices();
    }
}
