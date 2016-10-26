/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.firewallid.termcloud;

import com.firewallid.classification.FeatureExtraction;
import com.firewallid.io.HBaseRead;
import com.firewallid.io.HBaseWrite;
import com.firewallid.util.FIConfiguration;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.linalg.Vector;
import scala.Tuple2;

public class TermCloudBuilder extends TermCloud {

    public static final String STATUSTRUE = "firewallindonesia.status.true";

    public static final String CRAWL_SUFFIXNAME = "table.crawl.suffixname";
    public static final String HOST_COLUMN = "table.crawl.hostcolumn";
    public static final String CLASSIFICATION_COLUMN = "table.crawl.classificationcolumn";
    public static final String CLEANING_COLUMN = "table.crawl.cleaningcolumn";
    public static final String CLASSIFIED_COLUMN = "table.crawl.classifiedcolumn";
    public static final String TERMCLOUDSTATUS_COLUMN = "table.crawl.termcloudstatuscolumn";

    public TermCloudBuilder() {
        super();
    }

    /* Build termcloud for every title */
    public void buildTermCloud(JavaSparkContext jsc, JavaPairRDD<String, String> doc, String fileNameTitleTFIDF, String prefixTermCloudName, boolean all) throws IOException {
        FeatureExtraction fe = new FeatureExtraction(jsc);

        JavaPairRDD<String, List<String>> reduceByTitle = reduceByTitle(doc);
        JavaPairRDD<String, Vector> transformTFIDF = fe.transformTFIDF(reduceByTitle);
        JavaPairRDD<String, Vector> loadTitleTFIDF = loadTitleTFIDF(jsc, fileNameTitleTFIDF);
        JavaPairRDD<String, Vector> combineTFIDF = fe.combineTFIDF(transformTFIDF, loadTitleTFIDF);
        saveTitleTFIDF(combineTFIDF, fileNameTitleTFIDF);
        JavaPairRDD<String, List<Tuple2<Integer, Double>>> selectTopTFIDF = fe.selectTopTFIDF(combineTFIDF, conf.getInt(TOPN, 100));
        JavaPairRDD<String, List<Tuple2<String, Double>>> transformIndiceToFeature = fe.transformIndiceToFeature(selectTopTFIDF);
        saveTermCloud(transformIndiceToFeature, prefixTermCloudName);
        if (all) {
            saveTermCloudAll(transformIndiceToFeature, prefixTermCloudName);
        }
    }

    /* Set status for every row to true/success */
    public JavaPairRDD<String, String> setStatusTrueRow(JavaPairRDD<String, Map<String, String>> doc) {
        return doc.mapToPair((Tuple2<String, Map<String, String>> row) -> new Tuple2<String, String>(row._1, conf.get(STATUSTRUE)));
    }

    public void run(JavaSparkContext jsc, String type, String prefixTableName) throws IOException {
        conf.addResource(type + "-default.xml");

        String tableName = prefixTableName + conf.get(NAME_DELIMITER) + conf.get(CRAWL_SUFFIXNAME);
        String hostColumn = conf.get(HOST_COLUMN);
        String classificationColumn = conf.get(CLASSIFICATION_COLUMN);
        String cleaningColumn = conf.get(CLEANING_COLUMN);
        String classifiedColumn = conf.get(CLASSIFIED_COLUMN);
        String termcloudStatusColumn = conf.get(TERMCLOUDSTATUS_COLUMN);
        List<String> retColumns = Arrays.asList(hostColumn, classificationColumn, cleaningColumn);
        String tfidfFileNameHost = tableName + conf.get(NAME_DELIMITER) + "host" + conf.get(NAME_DELIMITER) + "tfidf";
        String tfidfFileNameLabel = tableName + conf.get(NAME_DELIMITER) + "label" + conf.get(NAME_DELIMITER) + "tfidf";

        /* Get data */
        JavaPairRDD<String, Map<String, String>> notTermCloudRow;
        if (isExistsTitleTFIDF(tfidfFileNameHost) && isExistsTitleTFIDF(tfidfFileNameLabel)) {
            notTermCloudRow = new HBaseRead().doRead(jsc, tableName, classifiedColumn, termcloudStatusColumn, retColumns);
        } else {
            notTermCloudRow = new HBaseRead().doRead(jsc, tableName, classifiedColumn, null, retColumns);
        }

        /* Build term cloud for host */
        JavaPairRDD<String, String> notTermCloudRowHost = notTermCloudRow.mapToPair(row -> new Tuple2(row._2.get(hostColumn), row._2.get(cleaningColumn)));
        buildTermCloud(jsc, notTermCloudRowHost, tfidfFileNameHost, tableName, false);

        /* Build term cloud for label */
        JavaPairRDD<String, String> notTermCloudRowLabel = notTermCloudRow.mapToPair(row -> new Tuple2(row._2.get(classificationColumn), row._2.get(cleaningColumn)));
        buildTermCloud(jsc, notTermCloudRowLabel, tfidfFileNameLabel, tableName, true);

        /* Set status */
        JavaPairRDD<String, String> setStatusTrueRow = setStatusTrueRow(notTermCloudRow);

        /* HBaseWrite */
        new HBaseWrite().save(tableName, setStatusTrueRow, termcloudStatusColumn);
    }

    public static void main(String[] args) throws IOException {
        /* Arguments */
        if (args.length < 2 || !Arrays.asList("website", "twitter", "facebook").contains(args[0])) {
            System.out.println("Arguments: website|twitter|facebook <tableId>");
            System.exit(-1);
        }

        String type = args[0];
        String tableId = args[1];

        /*Inisiasi*/
        JavaSparkContext jsc = new JavaSparkContext(FIConfiguration.createSparkConf(null));

        /* Run */
        new TermCloudBuilder().run(jsc, type, tableId);
    }
}
