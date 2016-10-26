/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.firewallid.cleaning;

import com.firewallid.util.FIConfiguration;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Arrays;
import org.apache.spark.api.java.JavaSparkContext;

/**
 *
 * @author andrenovelando@gmail.com
 */
public class Cleaner {

    public static Cleaning cleaning = new Cleaning();

    public static void main(String[] args) throws IOException, URISyntaxException {
        if (args.length < 2 || !Arrays.asList("website", "twitter", "facebook").contains(args[0])) {
            System.out.println("Arguments: website|twitter|facebook <tableId>");
            System.exit(-1);
        }

        String type = args[0];
        String prefixTableName = args[1];

        /*Inisiasi JavaSparkContext*/
        JavaSparkContext jsc = new JavaSparkContext(FIConfiguration.createSparkConf(new Class[]{IDFormalization.class, IDPOSTagger.class, IDRemoval.class, IDSentenceDetector.class}));

        /* Run cleaning */
        cleaning.run(jsc, type, prefixTableName);
    }
}
