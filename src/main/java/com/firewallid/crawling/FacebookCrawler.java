/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.firewallid.crawling;

import com.firewallid.util.FIConfiguration;
import java.io.IOException;
import org.apache.spark.api.java.JavaSparkContext;

/**
 *
 * @author andrenovelando@gmail.com
 */
public class FacebookCrawler {

    public static void main(String[] args) throws IOException {
        if (args.length < 1) {
            System.out.println("Argument: <tableId>");
            System.exit(-1);
        }

        String prefixTableName = args[0];
        JavaSparkContext jsc = new JavaSparkContext(FIConfiguration.createSparkConf(null));

        new FacebookIDSearch().run(prefixTableName, jsc);
        new FacebookCrawling().run(prefixTableName, jsc);
    }
}
