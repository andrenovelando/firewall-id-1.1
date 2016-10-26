/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.firewallid.util;

import com.firewallid.io.HBaseRead;
import java.io.Serializable;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;
import org.jsoup.Jsoup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

/**
 *
 * @author andrenovelando@gmail.com
 */
public class HTMLContent implements Serializable {

    public static final Logger LOG = LoggerFactory.getLogger(HTMLContent.class);

    public static final String PARTITIONS = "firewallindonesia.read.partitions";
    public static final String RETRIES = "firewallindonesia.util.download.retries";
    public static final String TIMEOUT = "firewallindonesia.util.download.timeout";

    int partitions;
    int retries;
    long timeout;

    public HTMLContent(Configuration firewallConf) {
        partitions = firewallConf.getInt(PARTITIONS, 48);
        retries = firewallConf.getInt(RETRIES, 10);
        timeout = firewallConf.getLong(TIMEOUT, 5);
    }

    public void download(JavaSparkContext jsc, String inFilePath) {
        /* Download all url in file */
        JavaPairRDD<String, String> download = jsc.textFile(inFilePath)
                .persist(StorageLevel.MEMORY_AND_DISK_SER_2())
                .repartition(partitions)
                .mapToPair((String url) -> {
                    String html = null;
                    for (int i = 1; i <= retries; i++) {
                        try {
                            html = Jsoup.connect(url).userAgent("Mozilla").get().outerHtml();
                            LOG.info("Download SUCCESS. -> " + url);
                            break;
                        } catch (Exception e) {
                            LOG.info("Download ERROR. Retries " + i + ". -> " + url);
                            Thread.sleep(timeout * 1000);
                        }
                    }
                    return new Tuple2(url, html);
                });

        JavaPairRDD<String, String> downloaded = download.filter((Tuple2<String, String> t) -> t._2 != null && !t._2.isEmpty());

        /* Save */
        downloaded.saveAsObjectFile(inFilePath + "-ObjectFile");
    }

    public static void main(String[] args) {
        String inFilePath = args[0];

        //Sparkcontext
        SparkConf conf = new SparkConf();
        conf.set("spark.scheduler.mode", "FAIR");
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        conf.registerKryoClasses(new Class[]{Tuple2.class, HTMLContent.class});
        JavaSparkContext jsc = new JavaSparkContext(conf);

        Configuration firewallConf = FIConfiguration.create();

        HTMLContent htmlContent = new HTMLContent(firewallConf);
        htmlContent.download(jsc, inFilePath);
    }
}
