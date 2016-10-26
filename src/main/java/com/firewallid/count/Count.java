/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.firewallid.count;

import com.firewallid.io.HBaseRead;
import com.firewallid.util.FIConfiguration;
import com.firewallid.util.FISQL;
import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import java.io.IOException;
import java.io.Serializable;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;
import scala.Tuple3;

/**
 *
 * @author andrenovelando@gmail.com
 */
public class Count implements Serializable {

    public static final Logger LOG = LoggerFactory.getLogger(Count.class);

    public static final String TEXT_DELIMITER = "firewallindonesia.file.textdelimiter";

    /* HBase */
    public static final String CRAWL_SUFFIXNAME = "table.crawl.suffixname";
    public static final String CRAWL_CRAWLING_COLUMN = "table.crawl.crawlingcolumn";
    public static final String CRAWL_CLEANING_COLUMN = "table.crawl.cleaningcolumn";
    public static final String CRAWL_CLASSIFICATION_COLUMN = "table.crawl.classificationcolumn";
    public static final String CRAWL_ANALYSIS_COLUMN = "table.crawl.analysiscolumn";
    public static final String CRAWL_HOST_COLUMN = "table.crawl.hostcolumn";

    public static final String COMMENT_SUFFIXNAME = "table.comment.suffixname";
    public static final String COMMENT_CLASSIFICATION_COLUMN = "table.comment.classificationcolumn";
    public static final String COMMENT_SENTIMENT_COLUMN = "table.comment.sentimentcolumn";
    public static final String COMMENT_HOST_COLUMN = "table.comment.hostcolumn";

    /* SQL */
    public static final String DRIVER_CLASS = "count.database.driverclass";
    public static final String DB_ADDRESS = "count.database.address";
    public static final String DB_USERNAME = "count.database.username";
    public static final String DB_PASSWORD = "count.database.password";
    public static final String MONITORING_TABLE = "count.database.table.monitoring";
    public static final String MONITORING_COLUMNS = "count.database.table.monitoring.columns"; // Value: taskId, crawling, cleaning, classification, analysis, type
    public static final String HOST_TABLE = "count.database.table.host";
    public static final String HOST_COLUMNS = "count.database.table.host.columns"; // Value: hostId, taskId, hostName
    public static final String CLASSIFICATION_TABLE = "count.database.table.classification";
    public static final String CLASSIFICATION_COLUMNS = "count.database.table.classification.columns"; // Value: hostId
    public static final String SENTIMENT_TABLE = "count.database.table.sentiment";
    public static final String SENTIMENT_COLUMNS = "count.database.table.sentiment.columns"; // Value: hostId, classification

    static JavaSparkContext jsc;
    static Configuration firewallConf;
    HBaseRead read;
    Connection conn;
    String type;

    public Count(JavaSparkContext jsc, String type) throws ClassNotFoundException, SQLException {
        Count.jsc = jsc;
        firewallConf = FIConfiguration.create();
        firewallConf.addResource("auth.xml");
        firewallConf.addResource(type + "-default.xml");
        read = new HBaseRead();
        Class.forName(firewallConf.get(DRIVER_CLASS));
        conn = DriverManager.getConnection(firewallConf.get(DB_ADDRESS), firewallConf.get(DB_USERNAME), firewallConf.get(DB_PASSWORD));
        this.type = type;
    }

    public void monitoring(String userId, String taskId) throws IOException, SQLException {
        String tableName = userId + "-" + taskId + firewallConf.get(CRAWL_SUFFIXNAME);
        String crawlingColumn = firewallConf.get(CRAWL_CRAWLING_COLUMN);
        String cleaningColumn = firewallConf.get(CRAWL_CLEANING_COLUMN);
        String classificationColumn = firewallConf.get(CRAWL_CLASSIFICATION_COLUMN);
        String analysisColumn = firewallConf.get(CRAWL_ANALYSIS_COLUMN);
        String monitoringTable = firewallConf.get(MONITORING_TABLE);
        String textDelimiter = firewallConf.get(TEXT_DELIMITER);
        String[] monitoringColumns = firewallConf.get(MONITORING_COLUMNS).split(textDelimiter);

        long crawling = read.doRead(jsc, tableName, crawlingColumn, null).count();
        long cleaning = read.doRead(jsc, tableName, cleaningColumn, null).count();
        long classification = read.doRead(jsc, tableName, classificationColumn, null).count();
        long analysis = read.doRead(jsc, tableName, analysisColumn, null).count();

        LOG.info(String.format("Task ID: %s, crawling=%d cleaning=%d classification=%d analysis=%d", taskId, crawling, cleaning, classification, analysis));

        /* SAVE */
        Map<String, String> updateConditions = new HashMap<>();
        updateConditions.put(monitoringColumns[0], taskId);

        Map<String, String> fields = new HashMap<>();
        fields.put(monitoringColumns[1], Long.toString(crawling));
        fields.put(monitoringColumns[2], Long.toString(cleaning));
        fields.put(monitoringColumns[3], Long.toString(classification));
        fields.put(monitoringColumns[4], Long.toString(analysis));
        fields.put(monitoringColumns[5], type);

        FISQL.updateRowInsertIfNotExist(conn, monitoringTable, updateConditions, fields);
    }

    public void classification(String userId, String taskId) throws IOException {
        String tableName = userId + "-" + taskId + firewallConf.get(CRAWL_SUFFIXNAME);
        String hostColumn = firewallConf.get(CRAWL_HOST_COLUMN);
        String classificationColumn = firewallConf.get(CRAWL_CLASSIFICATION_COLUMN);
        String textDelimiter = firewallConf.get(TEXT_DELIMITER);
        String hostTable = firewallConf.get(HOST_TABLE);
        String[] hostColumns = firewallConf.get(HOST_COLUMNS).split(textDelimiter);
        String classificationTable = firewallConf.get(CLASSIFICATION_TABLE);
        String[] classificationColumns = firewallConf.get(CLASSIFICATION_COLUMNS).split(textDelimiter);

        List<String> resultColumns = new ArrayList<>();
        if (type.equals("website")) {
            resultColumns.add(hostColumn);
        }
        resultColumns.add(classificationColumn);

        /* Get data from HBase */
        JavaPairRDD<String, Map<String, String>> readClassification = read.doRead(jsc, tableName, classificationColumn, null, resultColumns);

        JavaPairRDD<String, Iterable<Tuple2<String, Integer>>> hostClassScore = readClassification
                /* Formatting into key = unique pair (host, classification) and value = score*/
                .flatMapToPair((Tuple2<String, Map<String, String>> rowClassification) -> {
                    String host;
                    if (type.equals("website")) {
                        host = rowClassification._2.get(hostColumn);
                    } else {
                        host = type;
                    }
                    String classifications = rowClassification._2.get(classificationColumn);

                    List<Tuple2<Tuple2<String, String>, Integer>> hostClassificationList = Arrays.asList(classifications.split(textDelimiter)).parallelStream().map((String label) -> new Tuple2<Tuple2<String, String>, Integer>(new Tuple2<>(host, label), 1)).collect(Collectors.toList());

                    return hostClassificationList;
                })
                .reduceByKey((Integer score1, Integer score2) -> score1 + score2)
                /* Grouping based host */
                .mapToPair((Tuple2<Tuple2<String, String>, Integer> uniquePairScore) -> new Tuple2<String, Tuple2<String, Integer>>(uniquePairScore._1._1, new Tuple2<>(uniquePairScore._1._2, uniquePairScore._2)))
                .groupByKey();

        /* Save */
        hostClassScore.foreach((Tuple2<String, Iterable<Tuple2<String, Integer>>> toSave) -> {
            String host = toSave._1;
            String hostIdField = hostColumns[0];

            /* Get host's id */
            Map<String, String> hostFields = new HashMap<>();
            hostFields.put(hostColumns[1], taskId);
            hostFields.put(hostColumns[2], host);
            hostFields.put(hostColumns[3], userId);//delete
            String hostId = FISQL.getFirstFieldInsertIfNotExist(conn, hostTable, hostIdField, hostFields);

            /* Update */
            Map<String, String> updateConditions = new HashMap<>();
            updateConditions.put(classificationColumns[0], hostId);
            updateConditions.put(classificationColumns[1], taskId);//delete
            updateConditions.put(classificationColumns[2], userId);//delete
            Map<String, String> classificationFields = Lists.newArrayList(toSave._2).parallelStream().collect(Collectors.toMap(classificationScore -> classificationScore._1, classificationScore -> Integer.toString(classificationScore._2)));

            String log = Joiner.on(", ").withKeyValueSeparator("=").join(classificationFields);
            LOG.info(String.format("Task ID: %s. Host: %s(id=%s). Log: %s", taskId, host, hostId, log));

            FISQL.updateRowInsertIfNotExist(conn, classificationTable, updateConditions, classificationFields);
        });

    }

    public void sentiment(String userId, String taskId) throws IOException {
        String tableName = userId + "-" + taskId + firewallConf.get(COMMENT_SUFFIXNAME);
        String hostColumn = firewallConf.get(COMMENT_HOST_COLUMN);
        String classificationColumn = firewallConf.get(COMMENT_CLASSIFICATION_COLUMN);
        String sentimentColumn = firewallConf.get(COMMENT_SENTIMENT_COLUMN);
        String textDelimiter = firewallConf.get(TEXT_DELIMITER);
        String hostTable = firewallConf.get(HOST_TABLE);
        String[] hostColumns = firewallConf.get(HOST_COLUMNS).split(textDelimiter);
        String sentimentTable = firewallConf.get(SENTIMENT_TABLE);
        String[] sentimentColumns = firewallConf.get(SENTIMENT_COLUMNS).split(textDelimiter);

        List<String> resultColumns = new ArrayList<>();
        if (type.equals("website")) {
            resultColumns.add(hostColumn);
        }
        resultColumns.add(classificationColumn);
        resultColumns.add(sentimentColumn);

        /* Get data from HBase */
        JavaPairRDD<String, Map<String, String>> readSentiment = read.doRead(jsc, tableName, sentimentColumn, null, resultColumns);

        JavaPairRDD<Tuple2<String, String>, Iterable<Tuple2<String, Integer>>> hostSentScorePair = readSentiment
                /* Formatting into key = unique pair (host, classification, sentiment) and value = score*/
                .flatMapToPair((Tuple2<String, Map<String, String>> rowSentiment) -> {
                    String host;
                    if (type.equals("website")) {
                        host = rowSentiment._2.get(hostColumn);
                    } else {
                        host = type;
                    }
                    String classifications = rowSentiment._2.get(classificationColumn);
                    String sentiment = rowSentiment._2.get(sentimentColumn);

                    List<Tuple2<Tuple3<String, String, String>, Integer>> hostClassificationList = Arrays.asList(classifications.split(textDelimiter)).parallelStream().map((String classification) -> new Tuple2<Tuple3<String, String, String>, Integer>(new Tuple3<>(host, classification, sentiment), 1)).collect(Collectors.toList());

                    return hostClassificationList;
                })
                .reduceByKey((Integer score1, Integer score2) -> score1 + score2)
                /* Grouping based host-classification pair */
                .mapToPair((Tuple2<Tuple3<String, String, String>, Integer> uniquePairScore) -> new Tuple2<Tuple2<String, String>, Tuple2<String, Integer>>(new Tuple2<>(uniquePairScore._1._1(), uniquePairScore._1._2()), new Tuple2<>(uniquePairScore._1._3(), uniquePairScore._2)))
                .groupByKey();

        /* Save */
        hostSentScorePair.foreach((Tuple2<Tuple2<String, String>, Iterable<Tuple2<String, Integer>>> toSave) -> {
            String host = toSave._1._1;
            String hostIdField = hostColumns[0];
            String classification = toSave._1._1;

            /* Get host's id */
            Map<String, String> hostFields = new HashMap<>();
            hostFields.put(hostColumns[1], taskId);
            hostFields.put(hostColumns[2], host);
            String hostId = FISQL.getFirstFieldInsertIfNotExist(conn, hostTable, hostIdField, hostFields);

            /* Update */
            Map<String, String> updateConditions = new HashMap<>();
            updateConditions.put(sentimentColumns[0], hostId);
            updateConditions.put(sentimentColumns[1], classification);
            Map<String, String> sentimentFields = Lists.newArrayList(toSave._2).parallelStream().collect(Collectors.toMap(sentimentScore -> sentimentScore._1, sentimentScore -> Integer.toString(sentimentScore._2)));

            String log = Joiner.on(", ").withKeyValueSeparator("=").join(sentimentFields);
            LOG.info("Task ID: %s. Host: %s(id=%s). Classification: %s. Log: %s", taskId, host, hostId, classification, log);

            FISQL.updateRowInsertIfNotExist(conn, sentimentTable, updateConditions, sentimentFields);
        });
    }
}
