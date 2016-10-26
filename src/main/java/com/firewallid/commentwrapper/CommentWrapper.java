/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.firewallid.commentwrapper;

import com.firewallid.util.FIConfiguration;
import java.io.IOException;
import org.apache.spark.api.java.JavaSparkContext;

/**
 *
 * @author andrenovelando@gmail.com
 */
public class CommentWrapper {

    public static void main(String[] args) throws IOException {
        /* Arguments */
        String type = args[0];
        String prefixTableName = args[1];

        JavaSparkContext jsc = new JavaSparkContext(FIConfiguration.createSparkConf(null));

        switch (type) {
            case "website":
                new WebsiteComment().run(jsc, prefixTableName);
                break;
            case "twitter":
                new TwitterComment().run(jsc, prefixTableName);
                break;
            case "facebook":
                new FacebookComment().run(jsc, prefixTableName);
                break;
        }
    }
}
