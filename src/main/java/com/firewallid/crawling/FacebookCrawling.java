/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.firewallid.crawling;

import com.firewallid.base.Base;
import com.firewallid.io.HBaseRead;
import com.firewallid.io.HBaseTableUtils;
import com.firewallid.io.HBaseWrite;
import com.google.common.base.Joiner;
import facebook4j.Post;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

/**
 *
 * @author andrenovelando@gmail.com
 */
public class FacebookCrawling extends Base {

    public static final String NAME_DELIMITER = "firewallindonesia.name.delimiter";

    public static final String INTERVAL = "crawling.facebook.interval";

    public static final String CRAWL_SUFFIXNAME = "table.crawl.suffixname";
    public static final String CRAWL_FAMILYS = "table.crawl.familys";
    public static final String CRAWL_HOSTID_COLUMN = "table.crawl.hostidcolumn";
    public static final String CRAWL_CRAWLING_COLUMN = "table.crawl.crawlingcolumn";
    public static final String CRAWL_TIME_COLUMN = "table.crawl.timecolumn";
    public static final String CRAWL_AUTHORID_COLUMN = "table.crawl.authoridcolumn";
    public static final String CRAWL_AUTHORNAME_COLUMN = "table.crawl.authornamecolumn";

    public static final String HOST_SUFFIXNAME = "table.host.suffixname";
    public static final String HOST_TYPE_COLUMN = "table.host.typecolumn";
    public static final String HOST_CRAWLTIME_COLUMN = "table.host.crawltimecolumn";

    static FacebookApp facebookApp = new FacebookApp();

    public FacebookCrawling() {
        conf.addResource("facebook-default.xml");
    }

    /* Formatting group/page's post into HBaseWrite format */
    public Tuple2<String, Map<String, String>> formatOutput(String hostId, Post post) {
        Map<String, String> fieldOuputs = new HashMap<>();
        fieldOuputs.put(conf.get(CRAWL_HOSTID_COLUMN), hostId);

        List<String> candidateTexts = new ArrayList<>();
        candidateTexts.add(post.getMessage());
        candidateTexts.add(post.getDescription());
        candidateTexts.add(post.getCaption());
        candidateTexts.add(post.getStory());
        List<String> texts = candidateTexts.parallelStream().filter(text -> text != null && !text.isEmpty()).collect(Collectors.toList());
        String text = Joiner.on(". ").join(texts);
        fieldOuputs.put(conf.get(CRAWL_CRAWLING_COLUMN), text);

        String postTime;
        if (post.getCreatedTime() != null) {
            postTime = Long.toString(post.getCreatedTime().getTime());
        } else {
            postTime = Long.toString(post.getUpdatedTime().getTime());
        }
        fieldOuputs.put(conf.get(CRAWL_TIME_COLUMN), postTime);

        if (post.getFrom() != null) {
            String authorId = post.getFrom().getId();
            String authorName = post.getFrom().getName();
            fieldOuputs.put(conf.get(CRAWL_AUTHORID_COLUMN), authorId);
            fieldOuputs.put(conf.get(CRAWL_AUTHORNAME_COLUMN), authorName);
        }

        return new Tuple2<>(post.getId(), fieldOuputs);
    }

    /* Get latest group/page's posts */
    public List<Tuple2<String, Map<String, String>>> crawlPosts(String id, long sinceMillisTime) {
        return facebookApp.getFeeds(id, sinceMillisTime)
                .parallelStream().map(post -> formatOutput(id, post))
                .collect(Collectors.toList());
    }

    public JavaPairRDD<String, Map<String, String>> crawl(JavaPairRDD<String, Map<String, String>> ids) {
        String typeColumn = conf.get(HOST_TYPE_COLUMN);
        String crawltimeColumn = conf.get(HOST_CRAWLTIME_COLUMN);

        return ids
                .flatMapToPair((Tuple2<String, Map<String, String>> row) -> {
                    if (row._2.get(typeColumn).equals("user")) {
                        return null;
                    } else {
                        return crawlPosts(row._1, Long.parseLong(row._2.get(crawltimeColumn)));
                    }
                })
                .filter(row -> row != null);
    }

    public JavaPairRDD<String, Map<String, String>> filterInterval(JavaPairRDD<String, Map<String, String>> ids) {
        String crawltimeColumn = conf.get(HOST_CRAWLTIME_COLUMN);
        long interval = conf.getLong(INTERVAL, 3600000);

        return ids.filter(row -> (System.currentTimeMillis() - Long.parseLong(row._2.get(crawltimeColumn))) >= interval);
    }

    /* Update crawl time to current time */
    public JavaPairRDD<String, String> updateTime(JavaPairRDD<String, Map<String, String>> ids) {
        return ids.mapToPair(row -> new Tuple2<>(row._1, Long.toString(System.currentTimeMillis())));
    }

    public void run(String prefixTableName, JavaSparkContext jsc) throws IOException {
        String crawTable = prefixTableName + conf.get(NAME_DELIMITER) + conf.get(CRAWL_SUFFIXNAME);
        List<String> crawlFamilys = Arrays.asList(conf.getStrings(CRAWL_FAMILYS));
        String hostTable = prefixTableName + conf.get(NAME_DELIMITER) + conf.get(HOST_SUFFIXNAME);
        List<String> hostColumns = Arrays.asList(conf.get(HOST_TYPE_COLUMN), conf.get(HOST_CRAWLTIME_COLUMN));

        /* Create HBase table */
        HBaseTableUtils.createTable(crawTable, crawlFamilys);

        /* HBaseRead */
        JavaPairRDD<String, Map<String, String>> ids = new HBaseRead().doRead(jsc, hostTable, conf.get(HOST_TYPE_COLUMN), null, hostColumns);

        /* Preparation */
        JavaPairRDD<String, Map<String, String>> filterIds = filterInterval(ids);

        /* Crawl */
        JavaPairRDD<String, Map<String, String>> crawl = crawl(filterIds);
        JavaPairRDD<String, String> idsUpdate = updateTime(filterIds);

        /* Save */
        new HBaseWrite().save(crawTable, crawl, crawlFamilys);
        new HBaseWrite().save(hostTable, idsUpdate, conf.get(HOST_CRAWLTIME_COLUMN));
    }
}
