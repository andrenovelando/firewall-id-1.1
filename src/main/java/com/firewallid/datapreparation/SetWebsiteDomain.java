/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.firewallid.datapreparation;

import com.firewallid.io.HBaseRead;
import com.firewallid.io.HBaseWrite;
import com.firewallid.base.Base;
import com.firewallid.util.FIConfiguration;
import com.firewallid.util.FIUtils;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

/**
 *
 * @author andrenovelando@gmail.com
 */
public class SetWebsiteDomain extends Base {

    public JavaPairRDD<String, String> setDomainRow(JavaPairRDD<String, String> urlRow) {
        return urlRow.mapToPair((Tuple2<String, String> row) -> new Tuple2<>(row._1, FIUtils.extractDomain(row._2)))
                .filter((Tuple2<String, String> row) -> row._2 != null);
    }

    public static void main(String[] args) throws IOException {
        /* Arguments */
        String tableName = args[0];
        String urlColumn = args[1];
        String domainColumn = args[2];

        /*Inisiasi*/
        JavaSparkContext jSC = new JavaSparkContext(FIConfiguration.createSparkConf(null));
        Configuration firewallConf = FIConfiguration.create();
        SetWebsiteDomain setDomain = new SetWebsiteDomain();

        /* HBaseRead: Return only url column (exclude domain column) */
        JavaPairRDD<String, String> urlRow = new HBaseRead().doRead(jSC, tableName, urlColumn, domainColumn);

        /* Run: set domain */
        JavaPairRDD<String, String> domainRow = setDomain.setDomainRow(urlRow);

        /* HBaseWrite: save to HBase */
        new HBaseWrite().save(tableName, domainRow, domainColumn);
    }
}
