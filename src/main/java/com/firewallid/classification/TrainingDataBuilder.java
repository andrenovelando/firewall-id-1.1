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
public class TrainingDataBuilder {

    /* Arguments: upload|download <path to training data for download option> */
    public static void main(String[] args) throws IOException {
        if (args.length < 1) {
            System.out.println("Arguments: upload|download <path to training data for download option>");
            System.exit(-1);
        }

        String updown = args[0];
        String trainingDataPath;

        switch (updown) {
            case "upload":
                trainingDataPath = null;
                break;
            case "download":
                if (args.length == 1) {
                    trainingDataPath = "";
                } else {
                    trainingDataPath = args[1];
                }
                break;
            default:
                System.out.println("Arguments: upload|download <path to training data for download option>");
                System.exit(-1);
                return;
        }

        JavaSparkContext jsc = new JavaSparkContext(FIConfiguration.createSparkConf(null));
        new TrainingData().run(jsc, trainingDataPath);
    }
}
