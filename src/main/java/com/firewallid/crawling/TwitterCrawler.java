/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.firewallid.crawling;

import com.cybozu.labs.langdetect.LangDetectException;
import com.firewallid.util.FIConfiguration;
import java.io.IOException;
import java.net.URISyntaxException;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

/**
 *
 * @author andrenovelando@gmail.com
 */
public class TwitterCrawler {

    public static final String DURATION = "crawling.streaming.duration";

    public static void main(String[] args) throws IOException, LangDetectException, URISyntaxException {
        if (args.length < 1) {
            System.out.println("Argument: <tableId>");
            System.exit(-1);
        }

        String prefixTableName = args[0];

        Long duration = TwitterStreaming.conf.getLong(DURATION, 1000);

        JavaStreamingContext javaStreamingContext = new JavaStreamingContext(FIConfiguration.createSparkConf(null), new Duration(duration));

        new TwitterStreaming().run(javaStreamingContext, prefixTableName);

        javaStreamingContext.start();
        javaStreamingContext.awaitTermination();
    }
}
