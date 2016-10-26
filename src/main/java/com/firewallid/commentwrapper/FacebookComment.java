/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.firewallid.commentwrapper;

import com.firewallid.crawling.FacebookApp;
import com.firewallid.io.HBaseRead;
import com.firewallid.io.HBaseTableUtils;
import com.firewallid.io.HBaseWrite;
import com.firewallid.util.FIConfiguration;
import com.google.common.base.Joiner;
import facebook4j.Comment;
import java.io.IOException;
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

/**
 *
 * @author andrenovelando@gmail.com
 */
public class FacebookComment {

    public static final Logger LOG = LoggerFactory.getLogger(FacebookComment.class);

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
    public static final String COMMENT_TIME_COLUMN = "table.comment.timecolumn";
    public static final String COMMENT_REPLYTOID_COLUMN = "table.comment.replytoidcolumn";
    public static final String COMMENT_REPLYTOUSERID_COLUMN = "table.comment.replytouseridcolumn";
    public static final String COMMENT_REPLYTOUSERNAME_COLUMN = "table.comment.replytousernamecolumn";
    public static final String COMMENT_DEPTH_COLUMN = "table.comment.depthcolumn";
    public static final String COMMENT_BASEID_COLUMN = "table.comment.baseidcolumn";
    public static final String COMMENT_BASEUSERID_COLUMN = "table.comment.baseuseridcolumn";
    public static final String COMMENT_BASEUSERNAME_COLUMN = "table.comment.baseusernamecolumn";

    FacebookApp facebookApp;
    Configuration firewallConf;

    public FacebookComment() {
        facebookApp = new FacebookApp();
        firewallConf = FIConfiguration.create();
        firewallConf.addResource("facebook-default.xml");
    }

    /* Formatting output comment */
    public Map<String, String> formatOutput(Tuple2<Comment, Integer> comment, Comment commentBase) {
        Comment commentChild = comment._1();
        Comment commentParent = commentChild.getParent();

        Map<String, String> commentOut = new HashMap<>();
        commentOut.put("id", commentChild.getId());
        commentOut.put("text", commentChild.getMessage());
        commentOut.put("userId", commentChild.getFrom().getId());
        commentOut.put("userName", commentChild.getFrom().getName());
        commentOut.put("time", String.valueOf(commentChild.getCreatedTime().getTime()));
        commentOut.put("replytoId", commentParent.getId());
        commentOut.put("replytoUserId", commentParent.getFrom().getId());
        commentOut.put("replytoUserName", commentParent.getFrom().getName());
        commentOut.put("depth", String.valueOf(comment._2()));
        commentOut.put("baseId", commentBase.getId());
        commentOut.put("baseUserId", commentBase.getFrom().getId());
        commentOut.put("baseUserName", commentBase.getFrom().getName());

        LOG.info(Joiner.on(". ").withKeyValueSeparator(":").join(commentOut));

        return commentOut;
    }

    /* Get all comments/replies, include their childs */
    public List<Map<String, String>> getComments(String postId) {
        Comment commentBase = facebookApp.comment(postId);

        List<Map<String, String>> comments = facebookApp
                .getComments(postId)
                .parallelStream()
                .map(comment -> formatOutput(comment, commentBase))
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
        String commentTimeColumn = firewallConf.get(COMMENT_TIME_COLUMN);
        String commentReplytoIdColumn = firewallConf.get(COMMENT_REPLYTOID_COLUMN);
        String commentReplytoUserIdColumn = firewallConf.get(COMMENT_REPLYTOUSERID_COLUMN);
        String commentReplytoUserNameColumn = firewallConf.get(COMMENT_REPLYTOUSERNAME_COLUMN);
        String commentDepthColumn = firewallConf.get(COMMENT_DEPTH_COLUMN);
        String commentBaseIdColumn = firewallConf.get(COMMENT_BASEID_COLUMN);
        String commentBaseUserIdColumn = firewallConf.get(COMMENT_BASEUSERID_COLUMN);
        String commentBaseUserNameColumn = firewallConf.get(COMMENT_BASEUSERNAME_COLUMN);

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
                        commentField.put(commentTimeColumn, comment.get("time"));
                        commentField.put(commentReplytoIdColumn, comment.get("replytoId"));
                        commentField.put(commentReplytoUserIdColumn, comment.get("replytoUserId"));
                        commentField.put(commentReplytoUserNameColumn, comment.get("replytoUserName"));
                        commentField.put(commentDepthColumn, comment.get("depth"));
                        commentField.put(commentBaseIdColumn, comment.get("baseId"));
                        commentField.put(commentBaseUserIdColumn, comment.get("baseUserId"));
                        commentField.put(commentBaseUserNameColumn, comment.get("baseUserName"));

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
