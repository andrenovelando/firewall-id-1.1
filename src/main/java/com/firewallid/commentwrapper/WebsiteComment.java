/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.firewallid.commentwrapper;

import com.firewallid.base.Base;
import com.firewallid.io.HBaseRead;
import com.firewallid.io.HBaseTableUtils;
import com.firewallid.io.HBaseWrite;
import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import scala.Tuple2;

/**
 *
 * @author andrenovelando@gmail.com
 */
public class WebsiteComment extends Base {

    public static final String STATUSTRUE = "firewallindonesia.status.true";
    public static final String NAME_DELIMITER = "firewallindonesia.name.delimiter";

    public static final String CRAWL_SUFFIXNAME = "table.crawl.suffixname";
    public static final String CRAWL_HTML_COLUMN = "table.crawl.htmlcolumn";
    public static final String CRAWL_COMMENTWRAPPERSTATUS_COLUMN = "table.crawl.commentwrapperstatuscolumn";
    public static final String CRAWL_URL_COLUMN = "table.crawl.urlcolumn";
    public static final String CRAWL_DOMAIN_COLUMN = "table.crawl.hostcolumn";
    public static final String CRAWL_CLASSIFICATION_COLUMN = "table.crawl.classificationcolumn";
    public static final String CRAWL_CLASSIFIED_COLUMN = "table.crawl.classifiedcolumn";

    public static final String COMMENT_SUFFIXNAME = "table.comment.suffixname";
    public static final String COMMENT_FAMILYS = "table.comment.familys";
    public static final String COMMENT_AUTHORNAME_COLUMN = "table.comment.authornamecolumn";
    public static final String COMMENT_TEXT_COLUMN = "table.comment.textcolumn";
    public static final String COMMENT_DEPTH_COLUMN = "table.comment.depthcolumn";
    public static final String COMMENT_REPLYTONAME_COLUMN = "table.comment.replytonamecolumn";
    public static final String COMMENT_CLASSIFICATION_COLUMN = "table.comment.classificationcolumn";
    public static final String COMMENT_URL_COLUMN = "table.comment.urlcolumn";
    public static final String COMMENT_DOMAIN_COLUMN = "table.comment.domaincolumn";

    public static final String COMMENT_SECTOR_CLASS = "commentwrapper.website.commentsector.class";
    public static final String COMMENT_BLOCK_CLASS = "commentwrapper.website.commentblock.class";

    public WebsiteComment() {
        conf.addResource("website-default.xml");
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
        String csClass = conf.get(COMMENT_SECTOR_CLASS);
        String select;

        if (csClass.isEmpty()) {
            select = "ul,ol";
        } else {
            select = "div[class~=" + csClass + "],ol,ul";
        }

        return selectElement(document.body(), select);
    }

    public List<Element> commentBlockTwoOrMore(Element commentSector) {
        String cbClass = conf.get(COMMENT_BLOCK_CLASS);

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
        String cbClass = conf.get(COMMENT_BLOCK_CLASS);

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
        String crawlTableName = prefixTableName + conf.get(NAME_DELIMITER) + conf.get(CRAWL_SUFFIXNAME);
        String crawlHtmlColumn = conf.get(CRAWL_HTML_COLUMN);
        String crawlCommentWrapperStatusColumn = conf.get(CRAWL_COMMENTWRAPPERSTATUS_COLUMN);
        String crawlUrlColumn = conf.get(CRAWL_URL_COLUMN);
        String crawlDomainColumn = conf.get(CRAWL_DOMAIN_COLUMN);
        String crawlClassificationColumn = conf.get(CRAWL_CLASSIFICATION_COLUMN);
        String crawlClassifiedColumn = conf.get(CRAWL_CLASSIFIED_COLUMN);
        List<String> crawlColumnsList = Arrays.asList(crawlHtmlColumn, crawlUrlColumn, crawlDomainColumn, crawlClassificationColumn);

        String commentTableName = prefixTableName + conf.get(NAME_DELIMITER) + conf.get(COMMENT_SUFFIXNAME);
        List<String> commentFamilys = Arrays.asList(conf.getStrings(COMMENT_FAMILYS));
        String commentAuthorNameColumn = conf.get(COMMENT_AUTHORNAME_COLUMN);
        String commentTextColumn = conf.get(COMMENT_TEXT_COLUMN);
        String commentDepthColumn = conf.get(COMMENT_DEPTH_COLUMN);
        String commentReplytoNameColumn = conf.get(COMMENT_REPLYTONAME_COLUMN);
        String commentClassificationColumn = conf.get(COMMENT_CLASSIFICATION_COLUMN);
        String commentUrlColumn = conf.get(COMMENT_URL_COLUMN);
        String commentDomainColumn = conf.get(COMMENT_DOMAIN_COLUMN);

        /* Create comment table */
        HBaseTableUtils.createTable(commentTableName, commentFamilys);

        /* Get data from crawl table */
        JavaPairRDD<String, Map<String, String>> inComment = new HBaseRead().doRead(jsc, crawlTableName, crawlClassifiedColumn, crawlCommentWrapperStatusColumn, crawlColumnsList);

        /* Get all comment */
        JavaPairRDD<String, Map<String, String>> outComment = inComment
                .flatMapToPair(row -> {
                    List<Tuple2<String, Map<String, String>>> commentsFormat = getComments(row._2.get(crawlHtmlColumn))
                            .parallelStream()
                            .map(comment -> {
                                Map<String, String> commentFormat = new HashMap<>();
                                commentFormat.put(commentAuthorNameColumn, comment.get("user"));
                                commentFormat.put(commentTextColumn, comment.get("text"));
                                commentFormat.put(commentDepthColumn, comment.get("depth"));
                                commentFormat.put(commentReplytoNameColumn, comment.get("replyto"));
                                commentFormat.put(commentUrlColumn, comment.get(crawlUrlColumn));
                                commentFormat.put(commentDomainColumn, comment.get(crawlDomainColumn));
                                commentFormat.put(commentClassificationColumn, comment.get(crawlClassificationColumn));

                                return new Tuple2<>(row._1 + "-" + UUID.randomUUID(), commentFormat);
                            })
                            .collect(Collectors.toList());

                    return commentsFormat;
                });

        /* Set comment_status to true (crawl table) */
        String statusTrue = conf.get(STATUSTRUE);
        JavaPairRDD<String, String> updateStatus = inComment.mapToPair(row -> new Tuple2<>(row._1, statusTrue));

        /* Save ouput comment to comment table */
        new HBaseWrite().save(commentTableName, outComment, commentFamilys);

        /* Update crawl table */
        new HBaseWrite().save(crawlTableName, updateStatus, crawlCommentWrapperStatusColumn);
    }
}
