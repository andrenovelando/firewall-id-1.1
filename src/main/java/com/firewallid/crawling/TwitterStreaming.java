/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.firewallid.crawling;

import com.firewallid.base.Base;
import com.firewallid.datapreparation.IDLanguageDetector;
import com.firewallid.io.HBaseTableUtils;
import com.firewallid.io.HBaseWrite;
import com.google.common.base.Joiner;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.twitter.TwitterUtils;
import scala.Tuple2;
import twitter4j.Status;

/**
 *
 * @author andrenovelando@gmail.com
 */
public class TwitterStreaming extends Base {

    public static final String NAME_DELIMITER = "firewallindonesia.name.delimiter";

    public static final String CRAWL_SUFFIXNAME = "table.crawl.suffixname";
    public static final String CRAWL_FAMILYS = "table.crawl.familys";
    public static final String CRAWLING_COLUMN = "table.crawl.crawlingcolumn";
    public static final String TIME_COLUMN = "table.crawl.timecolumn";
    public static final String USERSCREENNAME_COLUMN = "table.crawl.screennamecolumn";
    public static final String USERNAME_COLUMN = "table.crawl.namecolumn";
    public static final String USERID_COLUMN = "table.crawl.useridcolumn";
    public static final String CONSUMER_KEY = "crawling.twitter.consumerkey";
    public static final String CONSUMER_SECRET = "crawling.twitter.consumersecret";
    public static final String ACCESS_TOKEN = "crawling.twitter.accesstoken";
    public static final String ACCESS_TOKEN_SECRET = "crawling.twitter.accesstokensecret";

    static IDLanguageDetector idLanguageDetector = new IDLanguageDetector();

    public TwitterStreaming() {
        conf.addResource("auth.xml");
        conf.addResource("twitter-default.xml");
    }

    public JavaPairDStream<String, Map<String, String>> streaming(JavaStreamingContext jsc) {
        /* Twitter Application */
        System.setProperty("twitter4j.oauth.consumerKey", conf.get(CONSUMER_KEY));
        System.setProperty("twitter4j.oauth.consumerSecret", conf.get(CONSUMER_SECRET));
        System.setProperty("twitter4j.oauth.accessToken", conf.get(ACCESS_TOKEN));
        System.setProperty("twitter4j.oauth.accessTokenSecret", conf.get(ACCESS_TOKEN_SECRET));

        JavaPairDStream<String, Map<String, String>> crawl = TwitterUtils
                /* Streaming */
                .createStream(jsc)
                /* Indonesia language filtering */
                .filter((Status status) -> idLanguageDetector.detect(status.getText()))
                /* Collect data */
                .mapToPair((Status status) -> {
                    Map<String, String> columns = new HashMap<>();
                    columns.put("text", status.getText());
                    columns.put("date", String.valueOf(status.getCreatedAt().getTime()));
                    columns.put("screenName", status.getUser().getScreenName());
                    columns.put("userName", status.getUser().getName());
                    columns.put("userId", String.valueOf(status.getUser().getId()));

                    LOG.info(String.format("id: %s. %s", status.getId(), Joiner.on(". ").withKeyValueSeparator(": ").join(columns)));

                    return new Tuple2<String, Map<String, String>>(String.valueOf(status.getId()), columns);
                });

        return crawl;
    }

    public void run(JavaStreamingContext jsc, String prefixTableName) throws IOException {
        String tableName = prefixTableName + conf.get(NAME_DELIMITER) + conf.get(CRAWL_SUFFIXNAME);
        String textColumn = conf.get(CRAWLING_COLUMN);
        String dateColumn = conf.get(TIME_COLUMN);
        String screenNameColumn = conf.get(USERSCREENNAME_COLUMN);
        String userNameColumn = conf.get(USERNAME_COLUMN);
        String userIdColumn = conf.get(USERID_COLUMN);
        List<String> familys = Arrays.asList(conf.getStrings(CRAWL_FAMILYS));

        /* Create HBase table */
        HBaseTableUtils.createTable(tableName, familys);

        /* Streaming */
        JavaPairDStream<String, Map<String, String>> streaming = streaming(jsc)
                .mapToPair(row -> {
                    Map<String, String> columns = new HashMap<>();
                    columns.put(textColumn, row._2().get("text"));
                    columns.put(dateColumn, row._2().get("date"));
                    columns.put(screenNameColumn, row._2().get("screenName"));
                    columns.put(userNameColumn, row._2().get("userName"));
                    columns.put(userIdColumn, row._2().get("userId"));
                    return new Tuple2<>(row._1(), columns);
                });

        /* Save to HBase */
        HBaseWrite hBaseWrite = new HBaseWrite();
        streaming.foreachRDD((JavaPairRDD<String, Map<String, String>> streamingRDD) -> {
            hBaseWrite.save(tableName, streamingRDD, familys);
            return null;
        });
    }
}
