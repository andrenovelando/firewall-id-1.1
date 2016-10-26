/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.firewallid.networkanalysis;

import com.firewallid.util.FIConfiguration;
import java.io.IOException;
import org.apache.spark.api.java.JavaSparkContext;

/**
 *
 * @author andrenovelando@gmail.com
 */
public class NetworkAnalyzer {

    public static void main(String[] args) throws IOException {
        /* Argument */
        String type = args[0];
        String prefixTableName = args[1];

        JavaSparkContext jsc = new JavaSparkContext(FIConfiguration.createSparkConf(null));

        new NetworkAnalysis(type).run(jsc, prefixTableName);
    }
}
