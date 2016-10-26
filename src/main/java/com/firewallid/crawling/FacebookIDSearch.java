/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.firewallid.crawling;

import com.cybozu.labs.langdetect.LangDetectException;
import com.firewallid.base.Base;
import com.firewallid.datapreparation.IDLanguageDetector;
import com.firewallid.io.HBaseTableUtils;
import com.firewallid.io.HBaseWrite;
import com.firewallid.io.HDFSRead;
import com.firewallid.util.FIFile;
import facebook4j.Group;
import facebook4j.Page;
import facebook4j.Post;
import facebook4j.ResponseList;
import facebook4j.User;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.commons.io.IOUtils;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;

/**
 *
 * @author andrenovelando@gmail.com
 */
public class FacebookIDSearch extends Base {

    public static final String NAME_DELIMITER = "firewallindonesia.name.delimiter";
    public static final String PARTITIONS = "firewallindonesia.read.partitions";

    public static final String KEYWORDS_PATH = "crawling.facebook.keywordspath";
    public static final String KEYWORDS = "crawling.facebook.keywords";
    public static final String SEARCH_LIMIT = "crawling.facebook.searchlimit";

    public static final String HOST_SUFFIXNAME = "table.host.suffixname";
    public static final String HOST_FAMILYS = "table.host.familys";
    public static final String HOST_TYPE_COLUMN = "table.host.typecolumn";
    public static final String HOST_NAME_COLUMN = "table.host.namecolumn";
    public static final String HOST_CRAWLTIME_COLUMN = "table.host.crawltimecolumn";

    static FacebookApp facebookApp = new FacebookApp();
    static IDLanguageDetector idLanguageDetector = new IDLanguageDetector();

    public FacebookIDSearch() {
        conf.addResource("facebook-default.xml");
    }

    public JavaRDD<String> loadKeywords(JavaSparkContext jsc) {
        return new HDFSRead().loadTextFile(jsc, conf.get(KEYWORDS_PATH));
    }

    /* Upload keywords from resource */
    public void uploadKeywords(JavaSparkContext jsc) throws IOException {
        int partitions = conf.getInt(PARTITIONS, 48);

        /* load training data */
        List<String> resource = IOUtils
                .readLines(getClass().getClassLoader().getResourceAsStream(conf.get(KEYWORDS)));

        Broadcast<List<String>> broadcastRes = jsc.broadcast(resource);

        JavaRDD<String> doc = jsc
                .parallelize(broadcastRes.value())
                .persist(StorageLevel.MEMORY_AND_DISK_SER_2())
                .repartition(partitions);

        save(doc);
    }

    /* Save keywords to hdfs */
    public void save(JavaRDD<String> doc) throws IOException {
        String keywordsPath = conf.get(KEYWORDS_PATH);
        String keywordsPathTemp = keywordsPath + "-tmp";

        FIFile.deleteExistHDFSPath(keywordsPath);
        FIFile.deleteExistHDFSPath(keywordsPathTemp);

        doc.saveAsTextFile(keywordsPathTemp);

        FIFile.copyMergeDeleteHDFS(keywordsPathTemp, keywordsPath);
    }

    /* Count post with indonesian language */
    public long countPostID(ResponseList<Post> posts) {
        return posts
                .parallelStream()
                .filter((Post post) -> {
                    try {
                        return idLanguageDetector.detect(post.getCaption())
                                || idLanguageDetector.detect(post.getDescription())
                                || idLanguageDetector.detect(post.getMessage())
                                || idLanguageDetector.detect(post.getName())
                                || idLanguageDetector.detect(post.getStory());
                    } catch (LangDetectException ex) {
                        LOG.error(ex.toString());
                        return false;
                    }
                })
                .count();
    }

    public long countPostID(List<String> posts) {
        return posts
                .parallelStream()
                .filter(post -> {
                    try {
                        return idLanguageDetector.detect(post);
                    } catch (LangDetectException ex) {
                        LOG.error(ex.toString());
                        return false;
                    }
                })
                .count();
    }

    /* Detect if majority posts (group/page) contains indonesian language */
    public boolean isPostsID(String id) {
        ResponseList<Post> posts = facebookApp.getFeeds(id, 10);
        long countPostID = countPostID(posts);
        return ((posts != null) && !posts.isEmpty() && ((double) countPostID / posts.size()) >= 0.5);
    }

    /* Detect if majority posts (user) contains indonesian language */
    public boolean isPostsID(User user) {
        List<String> userFeed = facebookApp.getUserFeeds(user.getId());
        if (userFeed == null) {
            return false;
        }
        long countPostID = countPostID(userFeed);
        return (!userFeed.isEmpty() && ((double) countPostID / userFeed.size()) >= 0.5);
    }

    /* Format output */
    public Tuple2<String, Map<String, String>> formatOutput(String type, String id, String name) {
        String nameColumn = conf.get(HOST_NAME_COLUMN);
        String typeColumn = conf.get(HOST_TYPE_COLUMN);
        String crawltimeColumn = conf.get(HOST_CRAWLTIME_COLUMN);

        Map<String, String> fieldOuputs = new HashMap<>();
        fieldOuputs.put(nameColumn, name);
        fieldOuputs.put(typeColumn, type);
        fieldOuputs.put(crawltimeColumn, Long.toString(System.currentTimeMillis()));

        LOG.info(String.format("type=%s id=%s name=%s", type, id, name));

        return new Tuple2<>(id, fieldOuputs);
    }

    public List<Tuple2<String, Map<String, String>>> searchGroup(String keyword) {
        /* Search Group with Language tag ID */
        List<Tuple2<String, Map<String, String>>> groups = facebookApp.searchGroupID(keyword, conf.getInt(SEARCH_LIMIT, 1000))
                .parallelStream()
                /* Select only open group */
                .filter((Group group) -> facebookApp.isGroupOpen(group))
                /* Select only group contains indonesian language */
                .filter((Group group) -> isPostsID(group.getId()))
                /* Get output */
                .map((Group group) -> formatOutput("group", group.getId(), group.getName()))
                .collect(Collectors.toList());

        return groups;
    }

    public List<Tuple2<String, Map<String, String>>> searchPage(String keyword) {
        /* Search Group with Language tag ID */
        List<Tuple2<String, Map<String, String>>> pages = facebookApp.searchPageID(keyword, conf.getInt(SEARCH_LIMIT, 1000))
                .parallelStream()
                /* Select only page contains indonesian language */
                .filter((Page page) -> isPostsID(page.getId()))
                /* Get output */
                .map((Page page) -> formatOutput("page", page.getId(), page.getName()))
                .collect(Collectors.toList());

        return pages;
    }

    public List<Tuple2<String, Map<String, String>>> searchUser(String keyword) {
        /* Search Group with Language tag ID */
        List<Tuple2<String, Map<String, String>>> users = facebookApp.searchUserID(keyword, conf.getInt(SEARCH_LIMIT, 1000))
                .stream()
                /* Select only user contains indonesian language */
                .filter(user -> isPostsID(user))
                /* Get output */
                .map(user -> formatOutput("user", user.getId(), user.getName()))
                .collect(Collectors.toList());

        return users;
    }

    public void run(String prefixTableName, JavaSparkContext jsc) throws IOException {
        String tableName = prefixTableName + conf.get(NAME_DELIMITER) + conf.get(HOST_SUFFIXNAME);
        List<String> familys = Arrays.asList(conf.getStrings(HOST_FAMILYS));

        /* Create HBase table */
        HBaseTableUtils.createTable(tableName, familys);

        /* Upload keywords if not exist */
        if (!FIFile.isExistsHDFSPath(conf.get(KEYWORDS_PATH))) {
            uploadKeywords(jsc);
        }

        /* Load keywords */
        JavaRDD<String> keywords = loadKeywords(jsc);

        /* Search */
        JavaPairRDD<String, Map<String, String>> IDs = keywords.flatMapToPair((String keyword) -> {
            List<Tuple2<String, Map<String, String>>> groups = searchGroup(keyword);
            List<Tuple2<String, Map<String, String>>> pages = searchPage(keyword);
            List<Tuple2<String, Map<String, String>>> users = searchUser(keyword);

            List<Tuple2<String, Map<String, String>>> ids = new ArrayList<>();
            ids.addAll(groups);
            ids.addAll(pages);
            ids.addAll(users);

            return ids;
        });

        /* Save */
        new HBaseWrite().save(tableName, IDs, familys);
    }
}
