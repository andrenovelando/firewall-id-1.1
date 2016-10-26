/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.firewallid.commentwrapper;

import com.firewallid.io.HBaseRead;
import com.firewallid.io.HBaseTableUtils;
import com.firewallid.io.HBaseWrite;
import com.firewallid.util.FIConfiguration;
import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

/**
 *
 * @author andrenovelando@gmail.com
 */
public class WebsiteComment {

    public static final Logger LOG = LoggerFactory.getLogger(WebsiteComment.class);

    public static final String STATUSTRUE = "firewallindonesia.status.true";

    public static final String CRAWL_SUFFIXNAME = "table.crawl.suffixname";
    public static final String CRAWL_COLUMNS = "table.crawl.columns"; // 0:html 1:comment_status 2:url 3:domain 4:classification 5:classified

    public static final String COMMENT_SUFFIXNAME = "table.comment.suffixname";
    public static final String COMMENT_FAMILYS = "table.comment.familys";
    public static final String COMMENT_COLUMNS = "table.comment.columns"; // 0:user 1:text 2:depth 3:replyto 4:classification 5:url 6:domain

    public static final String COMMENT_SECTOR_CLASS = "commentwrapper.website.commentsector.class";
    public static final String COMMENT_BLOCK_CLASS = "commentwrapper.website.commentblock.class";

    Configuration firewallConf;

    public WebsiteComment() {
        firewallConf = FIConfiguration.create();
        firewallConf.addResource("website-default.xml");
    }

    public List<Element> selectElement(Element element, String select) {
        List<Element> elementPredict = element.select(select).parallelStream()
                /* Exclude commentSector */
                .filter(cb -> !element.equals(cb))
                .collect(Collectors.toList());

        List<Element> elementPredictDisjoint = elementPredict.parallelStream()
                /* Disjoint comment block */
                .filter(cb -> new Elements(elementPredict).parallelStream().filter(cbTemp -> cbTemp.toString().replaceAll(" ", "").contains(cb.toString().replaceAll(" ", ""))).count() == 1)
                .collect(Collectors.toList());

        return elementPredictDisjoint;
    }

    /* Return candidate of comment sectors */
    public List<Element> commentSector(Document document) {
        String csClass = firewallConf.get(COMMENT_SECTOR_CLASS);
        String select;

        if (csClass.isEmpty()) {
            select = "ul,ol";
        } else {
            select = "div[class~=" + csClass + "],ol,ul";
        }

        return selectElement(document.body(), select);
    }

    public List<Element> commentBlockTwoOrMore(Element commentSector) {
        String cbClass = firewallConf.get(COMMENT_BLOCK_CLASS);

        List<Element> cbPredict = selectElement(commentSector, "div[class~=" + cbClass + "],li");

        if (cbPredict.isEmpty()) {
            return new ArrayList<>();
        }

        /* Asumption 1: commentSector has 2 or more commentBlock. Therefore, all commentblock locate in same level */
        List<Element> cbAsump = cbPredict.parallelStream()
                .collect(Collectors.groupingBy(cb -> cb.parent(), Collectors.mapping(cb -> cb, Collectors.toList())))
                /* Level checking */
                .entrySet().parallelStream().filter(parent_cb -> parent_cb.getValue().size() >= 2)
                /* Same level. Same tag. Same class */
                .flatMap(parent_cb -> parent_cb.getValue().parallelStream()
                        .filter(cb -> new Elements(parent_cb.getValue()).parallelStream().filter(cbTemp -> cbTemp.tagName().matches(cb.tagName()) && cbTemp.attr("class").split(" ")[0].matches(cb.attr("class").split(" ")[0])).count() >= 2)
                )
                /* User & text checking */
                .filter(cb -> !user(cb).isEmpty() && !text(cb).isEmpty())
                .collect(Collectors.toList());

        if (cbAsump.size() >= 2) {
            return cbAsump;
        }

        List<Element> cbAsumpChild = cbPredict.parallelStream()
                .map(cb -> commentBlockTwoOrMore(cb))
                .filter(cbl -> !cbl.isEmpty())
                .flatMap(cbl -> cbl.parallelStream())
                .collect(Collectors.toList());

        if (cbAsumpChild.size() >= 2) {
            return cbAsumpChild;
        }

        return new ArrayList<>();
    }

    public List<Element> commentBlockOne(Element commentSector) {
        String cbClass = firewallConf.get(COMMENT_BLOCK_CLASS);

        List<Element> cbPredict = selectElement(commentSector, "div[class~=" + cbClass + "],li");

        if (cbPredict.isEmpty()) {
            return new ArrayList<>();
        }

        List<Element> cbAsump = cbPredict.parallelStream()
                /* User & text checking */
                .filter(cb -> !user(cb).isEmpty() && !text(cb).isEmpty())
                .collect(Collectors.toList());

        return cbAsump;
    }

    /* Return candidate of comment blocks */
    public List<Element> commentBlock(Element commentSector) {
        List<Element> commentBlockTwoOrMore = commentBlockTwoOrMore(commentSector);
        if (!commentBlockTwoOrMore.isEmpty()) {
            return commentBlockTwoOrMore;
        }

        return commentBlockOne(commentSector);
    }

    public String user(Element commentBlock) {
        String regex1 = "(?i)(author|user)";
        String regex2 = "fn";
        String commentBlockText = commentBlock.text();
        List<Elements> userPredict = new ArrayList<>();
        userPredict.add(commentBlock.getElementsByTag("cite"));
        userPredict.add(commentBlock.select("div[class=" + regex2 + "]"));
        userPredict.add(commentBlock.select("div[class~=" + regex1 + "]"));
        userPredict.add(commentBlock.select("p[class=" + regex2 + "]"));
        userPredict.add(commentBlock.select("p[class~=" + regex1 + "]"));
        userPredict.add(commentBlock.select("span[class=" + regex2 + "]"));
        userPredict.add(commentBlock.select("span[class~=" + regex1 + "]"));
        userPredict.add(commentBlock.select("h4[class=" + regex2 + "]"));
        userPredict.add(commentBlock.select("h4[class~=" + regex1 + "]"));
        userPredict.add(commentBlock.select("strong"));

        Map<Integer, String> userPredictMap = userPredict
                .parallelStream()
                .filter(user -> !user.isEmpty())
                .map(user -> user.get(0).text())
                .collect(Collectors.toMap(user -> commentBlockText.indexOf(user), user -> user));

        if (userPredictMap.isEmpty()) {
            return "";
        }

        TreeMap<Integer, String> userPredictSorted = new TreeMap<>(userPredictMap);

        return userPredictSorted.get(userPredictSorted.firstKey());
    }

    public String text(Element commentBlock) {
        String regex = "(?i)(comment-content|comment-text|commentbody|comment-entry|commententry)";
        String commentBlockText = commentBlock.text();
        List<Elements> textPredict = new ArrayList<>();
        textPredict.add(commentBlock.select("div[class~=" + regex + "],p[class~=" + regex + "]"));
        textPredict.add(commentBlock.select("div[class~=" + regex + "],p[class~=" + regex + "]"));
        textPredict.add(commentBlock.select("p"));
        textPredict.add(commentBlock.select("p"));
        textPredict.add(commentBlock.select("p"));
        textPredict.add(commentBlock.select("blockquote"));

        Map<Integer, String> textPredictMap = textPredict
                .parallelStream()
                .filter(text -> !text.isEmpty())
                .map(text -> text.get(0).text())
                .collect(Collectors.toMap(text -> commentBlockText.indexOf(text), text -> text));

        if (textPredictMap.isEmpty()) {
            return "";
        }

        TreeMap<Integer, String> textPredictSorted = new TreeMap<>(textPredictMap);

        return textPredictSorted.get(textPredictSorted.firstKey());
    }

    public List<Element> child(Element commentBlock) {
        List<Element> childPredict = selectElement(commentBlock, "div[class~=(?i)(child|reply|replies)],li");

        if (childPredict.isEmpty()) {
            return new ArrayList<>();
        }

        List<Element> childAsump = childPredict.parallelStream()
                /* User & text checking */
                .filter(child -> !user(child).isEmpty() && !text(child).isEmpty())
                .collect(Collectors.toList());

        return childAsump;
    }

    public List<Map<String, String>> getComments(Element commentSector, String replyTo, int depth) {
        List<Map<String, String>> getcomments = commentBlock(commentSector)
                .parallelStream()
                .flatMap((Element cb) -> {
                    List<Map<String, String>> comments = new ArrayList<>();

                    Map<String, String> comment = new HashMap<>();
                    String user = user(cb);
                    comment.put("user", user);
                    comment.put("text", text(cb));
                    comment.put("depth", Integer.toString(depth));
                    comment.put("replyto", replyTo);

                    comments.add(comment);

                    List<Map<String, String>> commentChild = child(cb)
                            .parallelStream()
                            .flatMap(csChild -> getComments(csChild, user, depth + 1).parallelStream())
                            .collect(Collectors.toList());

                    comments.addAll(commentChild);

                    return comments.parallelStream();
                })
                .collect(Collectors.toList());

        return getcomments;
    }

    /* Get all comments from url
     * Format: {user, text, depth, replyto}
     */
    public List<Map<String, String>> getComments(URL url) throws IOException {
        Document document = Jsoup.connect(url.toString()).userAgent("Mozilla").get();
        List<Element> commentSectors = commentSector(document);
        List<Map<String, String>> comments = commentSectors
                .parallelStream()
                .flatMap(cs -> getComments(cs, "", 1).parallelStream())
                .collect(Collectors.toList());

        return comments;
    }

    public List<Map<String, String>> getComments(String htmlContent) throws IOException {
        Document document = Jsoup.parse(htmlContent);
        List<Element> commentSectors = commentSector(document);
        List<Map<String, String>> comments = commentSectors
                .parallelStream()
                .flatMap(cs -> getComments(cs, "", 1).parallelStream())
                .collect(Collectors.toList());

        return comments;
    }

    /* Run on SPARK */
    public void run(JavaSparkContext jsc, String prefixTableName) throws IOException {
        String crawlTableName = prefixTableName + firewallConf.get(CRAWL_SUFFIXNAME);
        String[] crawlColumns = firewallConf.getStrings(CRAWL_COLUMNS);
        List<String> crawlColumnsList = Arrays.asList(crawlColumns);
        String crawlHtmlColumn = crawlColumns[0];
        String crawlCommentStatusColumn = crawlColumns[1];
        String crawlUrlColumn = crawlColumns[2];
        String crawlDomainColumn = crawlColumns[3];
        String crawlClassificationColumn = crawlColumns[4];
        String crawlClassifiedColumn = crawlColumns[5];

        String commentTableName = prefixTableName + firewallConf.get(COMMENT_SUFFIXNAME);
        String[] commentFamilys = firewallConf.getStrings(COMMENT_FAMILYS);
        List<String> commentFamilysList = Arrays.asList(commentFamilys);
        String[] commentColumns = firewallConf.getStrings(COMMENT_COLUMNS);
        String commentUserColumn = commentColumns[0];
        String commentTextColumn = commentColumns[1];
        String commentDepthColumn = commentColumns[2];
        String commentReplytoColumn = commentColumns[3];
        String commentClassificationColumn = commentColumns[4];
        String commentUrlColumn = commentColumns[5];
        String commentDomainColumn = commentColumns[6];

        /* Create comment table */
        HBaseTableUtils.createTable(commentTableName, commentFamilysList);

        /* Get data from crawl table */
        JavaPairRDD<String, Map<String, String>> inComment = new HBaseRead().doRead(jsc, crawlTableName, crawlClassifiedColumn, crawlCommentStatusColumn, crawlColumnsList);

        /* Get all comment */
        JavaPairRDD<String, Map<String, String>> outComment = inComment
                .flatMapToPair(row -> {
                    List<Tuple2<String, Map<String, String>>> commentsFormat = getComments(row._2.get(crawlHtmlColumn))
                            .parallelStream()
                            .map(comment -> {
                                Map<String, String> commentFormat = new HashMap<>();
                                commentFormat.put(commentUserColumn, comment.get("user"));
                                commentFormat.put(commentTextColumn, comment.get("text"));
                                commentFormat.put(commentDepthColumn, comment.get("depth"));
                                commentFormat.put(commentReplytoColumn, comment.get("replyto"));
                                commentFormat.put(commentUrlColumn, comment.get(crawlUrlColumn));
                                commentFormat.put(commentDomainColumn, comment.get(crawlDomainColumn));
                                commentFormat.put(commentClassificationColumn, comment.get(crawlClassificationColumn));

                                return new Tuple2<>(row._1 + "-" + UUID.randomUUID(), commentFormat);
                            })
                            .collect(Collectors.toList());

                    return commentsFormat;
                });

        /* Set comment_status to true (crawl table) */
        String statusTrue = firewallConf.get(STATUSTRUE);
        JavaPairRDD<String, String> updateStatus = inComment.mapToPair(row -> new Tuple2<>(row._1, statusTrue));

        /* Save ouput comment to comment table */
        new HBaseWrite().save(commentTableName, outComment, commentFamilysList);

        /* Update crawl table */
        new HBaseWrite().save(crawlTableName, updateStatus, crawlCommentStatusColumn);
    }
}
