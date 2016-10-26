/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.firewallid.count;

import com.firewallid.util.FIConfiguration;
import java.io.IOException;
import java.sql.SQLException;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.api.java.JavaSparkContext;

/**
 *
 * @author andrenovelando@gmail.com
 */
public class Counter {

    public static void main(String[] args) throws ClassNotFoundException, SQLException, IOException {
        /* Args */
        String type = args[0];
        String taskId = args[1];
        String userId = args[2];

        /*Inisiasi JavaSparkContext*/
        JavaSparkContext jSC = new JavaSparkContext(FIConfiguration.createSparkConf(null));

        Count count = new Count(jSC, type);
//        count.monitoring(userId, taskId);
        count.classification(userId, taskId);
//        count.sentiment(commentTable, taskId);
    }
}
