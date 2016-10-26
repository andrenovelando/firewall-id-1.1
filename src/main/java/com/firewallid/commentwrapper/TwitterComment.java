/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.firewallid.commentwrapper;

import com.firewallid.crawling.TwitterApp;
import com.firewallid.io.HBaseRead;
import com.firewallid.io.HBaseTableUtils;
import com.firewallid.io.HBaseWrite;
import com.firewallid.util.FIConfiguration;
import com.google.common.base.Joiner;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;
import scala.Tuple3;
import twitter4j.Status;

/**
 *
 * @author andrenovelando
 */
public class TwitterComment {

    public static final Logger LOG = LoggerFactory.getLogger(TwitterComment.class);

    public static final String STATUSTRUE = "firewallindonesia.status.true";

    public static final String INTERVAL = "commentwrapper.interval";

    public static final String CRAWL_SUFFIXNAME = "table.crawl.suffixname";
    public static final String CRAWL_CLASSIFIED_COLUMN = "table.crawl.classifiedcolumn";
    public static final String CRAWL_COMMENTSTATUS_COLUMN = "table.crawl.commentstatuscolumn";
    public static final String CRAWL_CLASSIFICATION_COLUMN = "table.crawl.classificationcolumn";
    public static final String CRAWL_TIME_COLUMN = "table.crawl.timecolumn";

    public static final String COMMENT_SUFFIXNAME = "table.comment.suffixname";
    public static final String COMMENT_FAMILYS = "table.comment.familys";
    public static final String COMMENT_TEXT_COLUMN = "table.comment.textcolumn";
    public static final String COMMENT_USERID_COLUMN = "table.comment.useridcolumn";
    public static final String COMMENT_USERNAME_COLUMN = "table.comment.usernamecolumn";
    public static final String COMMENT_USERSCREENNAME_COLUMN = "table.comment.userscreennamecolumn";
    public static final String COMMENT_TIME_COLUMN = "table.comment.timecolumn";
    public static final String COMMENT_REPLYTOID_COLUMN = "table.comment.replytoidcolumn";
    public static final String COMMENT_REPLYTOUSERID_COLUMN = "table.comment.replytouseridcolumn";
    public static final String COMMENT_REPLYTOUSERSCREENNAME_COLUMN = "table.comment.replytouserscreennamecolumn";
    public static final String COMMENT_DEPTH_COLUMN = "table.comment.depthcolumn";
    public static final String COMMENT_BASEID_COLUMN = "table.comment.baseidcolumn";
    public static final String COMMENT_BASEUSERID_COLUMN = "table.comment.baseuseridcolumn";
    public static final String COMMENT_BASEUSERSCREENNAME_COLUMN = "table.comment.baseuserscreennamecolumn";

    TwitterApp twitterApp;
    Configuration firewallConf;

    public TwitterComment() {
        twitterApp = new TwitterApp();
        firewallConf = FIConfiguration.create();
        firewallConf.addResource("twitter-default.xml");
    }

    /* Formatting output comment */
    public Map<String, String> formatOutput(Tuple3<Status, Status, Integer> statusComment, Status statusBase) {
        Status statusChild = statusComment._1();
        Status statusParent = statusComment._2();

        Map<String, String> comment = new HashMap<>();
        comment.put("id", String.valueOf(statusChild.getId()));
        comment.put("text", statusChild.getText());
        comment.put("userId", String.valueOf(statusChild.getUser().getId()));
        comment.put("userName", statusChild.getUser().getName());
        comment.put("userScreenName", statusChild.getUser().getScreenName());
        comment.put("time", String.valueOf(statusChild.getCreatedAt().getTime()));
        comment.put("replytoId", String.valueOf(statusParent.getId()));
        comment.put("replytoUserId", String.valueOf(statusParent.getUser().getId()));
        comment.put("replytoUserScreenName", statusParent.getUser().getScreenName());
        comment.put("depth", String.valueOf(statusComment._3()));
        comment.put("baseId", String.valueOf(statusBase.getId()));
        comment.put("baseUserId", String.valueOf(statusBase.getUser().getId()));
        comment.put("baseUserScreenName", statusBase.getUser().getScreenName());

        LOG.info(Joiner.on(". ").withKeyValueSeparator(":").join(comment));

        return comment;
    }

    /* Get all comments/replies, include their childs */
    public List<Map<String, String>> getComments(String tweetId) {
        Status statusBase = twitterApp.status(tweetId);
        List<Map<String, String>> comments = twitterApp
                .replyToTree(statusBase, 1)
                .parallelStream()
                .map(status -> formatOutput(status, statusBase))
                .collect(Collectors.toList());

        return comments;
    }

    /* Running on spark */
    public void run(JavaSparkContext jsc, String prefixTableName) throws IOException {
        long interval = firewallConf.getLong(INTERVAL, 86400000);

        String crawlTableName = prefixTableName + firewallConf.get(CRAWL_SUFFIXNAME);
        String crawlClassifiedColumn = firewallConf.get(CRAWL_CLASSIFIED_COLUMN);
        String crawlCommentStatusColumn = firewallConf.get(CRAWL_COMMENTSTATUS_COLUMN);
        String crawlClassificationColumn = firewallConf.get(CRAWL_CLASSIFICATION_COLUMN);
        String crawlTimeColumn = firewallConf.get(CRAWL_TIME_COLUMN);
        List<String> crawlColumns = Arrays.asList(crawlClassificationColumn, crawlTimeColumn);
        String commentTableName = prefixTableName + firewallConf.get(COMMENT_SUFFIXNAME);
        String[] commentFamilys = firewallConf.getStrings(COMMENT_FAMILYS);
        List<String> commentFamilysList = Arrays.asList(commentFamilys);
        String commentTextColumn = firewallConf.get(COMMENT_TEXT_COLUMN);
        String commentUserIdColumn = firewallConf.get(COMMENT_USERID_COLUMN);
        String commentUserNameColumn = firewallConf.get(COMMENT_USERNAME_COLUMN);
        String commentUserScreenNameColumn = firewallConf.get(COMMENT_USERSCREENNAME_COLUMN);
        String commentTimeColumn = firewallConf.get(COMMENT_TIME_COLUMN);
        String commentReplytoIdColumn = firewallConf.get(COMMENT_REPLYTOID_COLUMN);
        String commentReplytoUserIdColumn = firewallConf.get(COMMENT_REPLYTOUSERID_COLUMN);
        String commentReplytoUserScreenNameColumn = firewallConf.get(COMMENT_REPLYTOUSERSCREENNAME_COLUMN);
        String commentDepthColumn = firewallConf.get(COMMENT_DEPTH_COLUMN);
        String commentBaseIdColumn = firewallConf.get(COMMENT_BASEID_COLUMN);
        String commentBaseUserIdColumn = firewallConf.get(COMMENT_BASEUSERID_COLUMN);
        String commentBaseUserScreenNameColumn = firewallConf.get(COMMENT_BASEUSERSCREENNAME_COLUMN);

        /* Create comment table */
        HBaseTableUtils.createTable(commentTableName, commentFamilysList);

        /* Get tweet from crawl table */
        JavaPairRDD<String, Map<String, String>> inComment = new HBaseRead()
                .doRead(jsc, crawlTableName, crawlClassifiedColumn, crawlCommentStatusColumn, crawlColumns)
                /* Select row which interval is reached */
                .filter(row -> System.currentTimeMillis() - Long.parseLong(row._2.get(crawlTimeColumn)) >= interval);

        /* Get comment */
        JavaPairRDD<String, Map<String, String>> outComment = inComment.flatMapToPair(row -> {
            List<Tuple2<String, Map<String, String>>> comments = getComments(row._1)
                    .parallelStream()
                    .map(comment -> {
                        Map<String, String> commentField = new HashMap<>();
                        commentField.put(commentTextColumn, comment.get("text"));
                        commentField.put(commentUserIdColumn, comment.get("userId"));
                        commentField.put(commentUserNameColumn, comment.get("userName"));
                        commentField.put(commentUserScreenNameColumn, comment.get("userScreenName"));
                        commentField.put(commentTimeColumn, comment.get("time"));
                        commentField.put(commentReplytoIdColumn, comment.get("replytoId"));
                        commentField.put(commentReplytoUserIdColumn, comment.get("replytoUserId"));
                        commentField.put(commentReplytoUserScreenNameColumn, comment.get("replytoUserScreenName"));
                        commentField.put(commentDepthColumn, comment.get("depth"));
                        commentField.put(commentBaseIdColumn, comment.get("baseId"));
                        commentField.put(commentBaseUserIdColumn, comment.get("baseUserId"));
                        commentField.put(commentBaseUserScreenNameColumn, comment.get("baseUserScreenName"));

                        return new Tuple2<>(comment.get("id"), commentField);
                    })
                    .collect(Collectors.toList());

            return comments;
        });

        /* Set comment_status to true (crawl table) */
        String statusTrue = firewallConf.get(STATUSTRUE);
        JavaPairRDD<String, String> updateStatus = inComment.mapToPair(row -> new Tuple2<>(row._1, statusTrue));

        /* Save comment to comment table */
        new HBaseWrite().save(commentTableName, outComment, commentFamilysList);

        /* Update status comment_status to crawl table */
        new HBaseWrite().save(crawlTableName, updateStatus, crawlCommentStatusColumn);
    }
}
