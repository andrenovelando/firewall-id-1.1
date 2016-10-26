/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.firewalld.sentimentanalysis;

import com.firewallid.io.HBaseRead;
import com.firewallid.io.HBaseWrite;
import com.firewallid.util.FIConfiguration;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

/**
 *
 * @author andrenovelando
 */
public class SentimentAnalyzer {

    public static final Logger LOG = LoggerFactory.getLogger(SentimentAnalyzer.class);

    public static final String COMMENT_SUFFIXNAME = "table.comment.suffixname";
    public static final String TEXT_COLUMN = "table.comment.textcolumn";
    public static final String SENTIMENT_COLUMN = "table.comment.sentimentcolumn";

    static SentimentAnalysis sentimentAnalysis = new SentimentAnalysis();
    Configuration firewallConf;

    public SentimentAnalyzer(String type) {
        firewallConf = FIConfiguration.create();
        firewallConf.addResource(type + "-default.xml");
    }

    public void run(JavaSparkContext jsc, String prefixTableName) throws IOException {
        String tableName = prefixTableName + firewallConf.get(COMMENT_SUFFIXNAME);
        String textColumn = firewallConf.get(TEXT_COLUMN);
        String sentimentColumn = firewallConf.get(SENTIMENT_COLUMN);

        JavaPairRDD<String, String> inSentiment = new HBaseRead().doRead(jsc, tableName, textColumn, sentimentColumn);

        JavaPairRDD<String, String> outSentiment = inSentiment.mapToPair(row -> new Tuple2<>(row._1, sentimentAnalysis.detect(row._2)));

        new HBaseWrite().save(tableName, outSentiment, sentimentColumn);
    }

    public static void main(String[] args) throws IOException {
        String type = args[0];
        String prefixTableName = args[1];

        JavaSparkContext jsc = new JavaSparkContext(FIConfiguration.createSparkConf(null));

        new SentimentAnalyzer(type).run(jsc, prefixTableName);
    }
}
