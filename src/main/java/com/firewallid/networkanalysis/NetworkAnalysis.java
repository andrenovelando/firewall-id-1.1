/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.firewallid.networkanalysis;

import com.codepoetics.protonpack.StreamUtils;
import com.firewallid.io.HBaseRead;
import com.firewallid.io.HBaseWrite;
import com.firewallid.util.FIConfiguration;
import com.firewallid.util.FIFile;
import com.firewallid.util.FIUtils;
import com.google.common.collect.Lists;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.commons.lang.StringEscapeUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

/**
 *
 * @author andrenovelando@gmail.com
 */
public class NetworkAnalysis {

    public static final Logger LOG = LoggerFactory.getLogger(NetworkAnalysis.class);

    public static final String PARTITIONS = "firewallindonesia.read.partitions";
    public static final String DOC_DELIMITER = "firewallindonesia.file.linedelimiter";
    public static final String NAME_DELIMITER = "firewallindonesia.name.delimiter";
    public static final String STATUSTRUE = "firewallindonesia.status.true";

    public static final String COMMENT_SUFFIXNAME = "table.comment.suffixname";
    public static final String COMMENT_USERNAME_COLUMN = "table.comment.usernamecolumn";
    public static final String COMMENT_REPLYTOUSERNAME_COLUMN = "table.comment.replytousernamecolumn";
    public static final String COMMENT_HOST_COLUMN = "table.comment.hostcolumn";
    public static final String COMMENT_NETWORKSTATUS_COLUMN = "table.comment.networkstatuscolumn";

    public static final String NETWORKANALYSIS_FOLDER = "networkanalysis.folder";

    Configuration firewallConf;
    String type;

    public NetworkAnalysis(String type) {
        firewallConf = FIConfiguration.create();
        this.type = type;
    }

    /* Return true if temporary file exists */
    public boolean isExistsTempFileNode(String prefixTempFileName) throws IOException {
        String nodePath = FIFile.generateFullPath(firewallConf.get(NETWORKANALYSIS_FOLDER), prefixTempFileName + firewallConf.get(NAME_DELIMITER) + "node");

        return FIFile.isExistsHDFSPath(nodePath);
    }

    public boolean isExistsTempFileEdge(String prefixTempFileName) throws IOException {
        String edgePath = FIFile.generateFullPath(firewallConf.get(NETWORKANALYSIS_FOLDER), prefixTempFileName + firewallConf.get(NAME_DELIMITER) + "edge");

        return FIFile.isExistsHDFSPath(edgePath);
    }

    /* Save temporary file */
    public void saveTempFileNode(JavaPairRDD<String, List<Tuple2<String, Long>>> nodes, String prefixTempFileName) throws IOException {
        String nodePath = FIFile.generateFullPath(firewallConf.get(NETWORKANALYSIS_FOLDER), prefixTempFileName + firewallConf.get(NAME_DELIMITER) + "node");

        FIFile.deleteExistHDFSPath(nodePath);
        nodes.saveAsObjectFile(nodePath);

        LOG.info("Temporary file node saved");
    }

    public void saveTempFileEdge(JavaPairRDD<String, Set<Tuple2<String, String>>> edges, String prefixTempFileName) throws IOException {
        String edgePath = FIFile.generateFullPath(firewallConf.get(NETWORKANALYSIS_FOLDER), prefixTempFileName + firewallConf.get(NAME_DELIMITER) + "edge");

        FIFile.deleteExistHDFSPath(edgePath);
        edges.saveAsObjectFile(edgePath);

        LOG.info("Temporary file edge saved");
    }

    /* Load temporary file */
    public JavaPairRDD<String, List<Tuple2<String, Long>>> loadTempFileNode(JavaSparkContext jsc, String prefixTempFileName) throws IOException {
        if (!isExistsTempFileNode(prefixTempFileName)) {
            return null;
        }

        String nodePath = FIFile.generateFullPath(firewallConf.get(NETWORKANALYSIS_FOLDER), prefixTempFileName + firewallConf.get(NAME_DELIMITER) + "node");
        int partitions = firewallConf.getInt(PARTITIONS, 48);

        JavaPairRDD<String, List<Tuple2<String, Long>>> nodes = jsc
                .objectFile(nodePath)
                .persist(StorageLevel.MEMORY_AND_DISK_SER_2())
                .repartition(partitions)
                .mapToPair(nodeOjt -> {
                    Tuple2<String, List<Tuple2<String, Long>>> node = (Tuple2<String, List<Tuple2<String, Long>>>) nodeOjt;
                    return new Tuple2<>(node._1(), node._2());
                });

        LOG.info("Temporary file node loaded");

        return nodes;
    }

    public JavaPairRDD<String, Set<Tuple2<String, String>>> loadTempFileEdge(JavaSparkContext jsc, String prefixTempFileName) throws IOException {
        if (!isExistsTempFileEdge(prefixTempFileName)) {
            return null;
        }

        String edgePath = FIFile.generateFullPath(firewallConf.get(NETWORKANALYSIS_FOLDER), prefixTempFileName + firewallConf.get(NAME_DELIMITER) + "edge");
        int partitions = firewallConf.getInt(PARTITIONS, 48);

        JavaPairRDD<String, Set<Tuple2<String, String>>> edges = jsc
                .objectFile(edgePath)
                .persist(StorageLevel.MEMORY_AND_DISK_SER_2())
                .repartition(partitions)
                .mapToPair(edgeOjt -> {
                    Tuple2<String, Set<Tuple2<String, String>>> edge = (Tuple2<String, Set<Tuple2<String, String>>>) edgeOjt;
                    return new Tuple2<>(edge._1(), edge._2());
                });

        LOG.info("Temporary file edge loaded");

        return edges;
    }

    /*
     * Return node with its size per title.
     * Input: (id, {title: ..., userName: ..., replytoUserName: ...})
     */
    public JavaPairRDD<String, List<Tuple2<String, Long>>> nodes(JavaPairRDD<String, Map<String, String>> doc) {
        JavaPairRDD<String, List<Tuple2<String, Long>>> titleNodes = doc
                .mapToPair(row -> {
                    List<String> nodes = new ArrayList<>();
                    nodes.add(row._2().get("userName"));
                    nodes.add(row._2().get("replytoUserName"));

                    return new Tuple2<>(row._2().getOrDefault("title", "all"), nodes);
                })
                .reduceByKey((nodes1, nodes2) -> FIUtils.combineList(nodes1, nodes2))
                .mapToPair(nodes -> new Tuple2<>(nodes._1(), FIUtils.reduceList(nodes._2())));

        return titleNodes;
    }

    public JavaPairRDD<String, List<Tuple2<String, Long>>> combineNodes(JavaPairRDD<String, List<Tuple2<String, Long>>> nodes1, JavaPairRDD<String, List<Tuple2<String, Long>>> nodes2) {
        LOG.info("Combine Nodes starting...");

        if (nodes1 == null) {
            return nodes2;
        }

        if (nodes2 == null) {
            return nodes1;
        }

        JavaPairRDD<String, List<Tuple2<String, Long>>> combine = nodes1.union(nodes2)
                .reduceByKey((node1, node2) -> FIUtils.sumList(node1, node2)
                        .parallelStream()
                        .map(node -> new Tuple2<>(node._1(), (Long) (long) (double) node._2()))
                        .collect(Collectors.toList()));

        return combine;
    }

    /*
     * Return edges per title.
     * Input: (id, {title: ..., userName: ..., replytoUserName: ...})
     */
    public JavaPairRDD<String, Set<Tuple2<String, String>>> edges(JavaPairRDD<String, Map<String, String>> doc) {
        JavaPairRDD<String, Set<Tuple2<String, String>>> titleEdges = doc
                .mapToPair(row -> new Tuple2<>(row._2().getOrDefault("title", "all"),
                        new Tuple2<>(row._2().get("userName"), row._2().get("replytoUserName"))))
                .groupByKey()
                .mapToPair(edges -> new Tuple2<>(edges._1(),
                        FIUtils.listToSet(Lists.newArrayList(edges._2()))));

        return titleEdges;
    }

    public JavaPairRDD<String, Set<Tuple2<String, String>>> combineEdges(JavaPairRDD<String, Set<Tuple2<String, String>>> nodes1, JavaPairRDD<String, Set<Tuple2<String, String>>> nodes2) {
        LOG.info("Combine Edges starting...");

        if (nodes1 == null) {
            return nodes2;
        }

        if (nodes2 == null) {
            return nodes1;
        }

        JavaPairRDD<String, Set<Tuple2<String, String>>> combine = nodes1.union(nodes2)
                .reduceByKey((node1, node2) -> {
                    node1.addAll(node2);
                    return node1;
                });

        return combine;
    }

    /* Save network file (nodes & edges) */
    public void saveNAFileNodes(JavaPairRDD<String, List<Tuple2<String, Long>>> nodes, String prefixFileName) {
        String docDelimiter = StringEscapeUtils.unescapeJava(firewallConf.get(DOC_DELIMITER));
        String naFolder = firewallConf.get(NETWORKANALYSIS_FOLDER);
        String nameDelimiter = firewallConf.get(NAME_DELIMITER);

        nodes
                .mapToPair(node -> new Tuple2<>(node._1(),
                        "id" + docDelimiter + "node" + docDelimiter + "size"
                        + System.lineSeparator()
                        + StreamUtils.zipWithIndex(node._2().parallelStream())
                        .parallel()
                        .map(nodeItem -> nodeItem.getIndex() + docDelimiter + nodeItem.getValue()._1() + docDelimiter + nodeItem.getValue()._2())
                        .collect(Collectors.joining(System.lineSeparator()))))
                .foreach(node -> FIFile.writeStringToHDFSFile(
                        FIFile.generateFullPath(naFolder,
                                prefixFileName + nameDelimiter + node._1() + nameDelimiter + "node.csv"),
                        node._2()));
    }

    public void saveNAFileEdges(JavaPairRDD<String, Set<Tuple2<String, String>>> edges, String prefixFileName) {
        String docDelimiter = StringEscapeUtils.unescapeJava(firewallConf.get(DOC_DELIMITER));
        String naFolder = firewallConf.get(NETWORKANALYSIS_FOLDER);
        String nameDelimiter = firewallConf.get(NAME_DELIMITER);

        edges
                .mapToPair(edge -> new Tuple2<>(edge._1(),
                        "id" + docDelimiter + "node1" + docDelimiter + "node2"
                        + System.lineSeparator()
                        + StreamUtils.zipWithIndex(edge._2().parallelStream())
                        .parallel()
                        .map(edgeItem -> edgeItem.getIndex() + docDelimiter + edgeItem.getValue()._1() + docDelimiter + edgeItem.getValue()._2())
                        .collect(Collectors.joining(System.lineSeparator()))))
                .foreach(edge -> FIFile.writeStringToHDFSFile(
                        FIFile.generateFullPath(naFolder,
                                prefixFileName + nameDelimiter + edge._1() + nameDelimiter + "edge.csv"),
                        edge._2()));
    }

    public void run(JavaSparkContext jsc, String prefixName) throws IOException {
        String commentTableName = prefixName + firewallConf.get(COMMENT_SUFFIXNAME);
        String commentUserNameColumn = firewallConf.get(COMMENT_USERNAME_COLUMN);
        String commentReplytoUserNameColumn = firewallConf.get(COMMENT_REPLYTOUSERNAME_COLUMN);
        String commentNetworkStatusColumn = firewallConf.get(COMMENT_NETWORKSTATUS_COLUMN);

        List<String> commentColumns;
        if (type.equals("website")) {
            String commentHostColumn = firewallConf.get(COMMENT_HOST_COLUMN);
            commentColumns = Arrays.asList(commentHostColumn, commentUserNameColumn, commentReplytoUserNameColumn);
        } else {
            commentColumns = Arrays.asList(commentUserNameColumn, commentReplytoUserNameColumn);

        }

        /* HBaseRead comment table */
        JavaPairRDD<String, Map<String, String>> inNetwork;
        if (isExistsTempFileNode(prefixName) && isExistsTempFileEdge(prefixName)) {
            inNetwork = new HBaseRead().doRead(jsc, commentTableName, commentUserNameColumn, null, commentColumns);
        } else {
            inNetwork = new HBaseRead().doRead(jsc, commentTableName, commentUserNameColumn, commentNetworkStatusColumn, commentColumns);
        }

        /* Build network */
        JavaPairRDD<String, List<Tuple2<String, Long>>> nodesNew = nodes(inNetwork);
        JavaPairRDD<String, List<Tuple2<String, Long>>> nodesPrev = loadTempFileNode(jsc, prefixName);
        JavaPairRDD<String, List<Tuple2<String, Long>>> nodes = combineNodes(nodesNew, nodesPrev);

        JavaPairRDD<String, Set<Tuple2<String, String>>> edgesNew = edges(inNetwork);
        JavaPairRDD<String, Set<Tuple2<String, String>>> edgesPrev = loadTempFileEdge(jsc, prefixName);
        JavaPairRDD<String, Set<Tuple2<String, String>>> edges = combineEdges(edgesNew, edgesPrev);

        /* Save temporary network */
        saveTempFileNode(nodes, prefixName);
        saveTempFileEdge(edges, prefixName);

        /* Create file network */
        saveNAFileNodes(nodes, prefixName);
        saveNAFileEdges(edges, prefixName);

        /* Update status comment table */
        String statusTrue = firewallConf.get(STATUSTRUE);
        JavaPairRDD<String, String> updateStatus = inNetwork.mapToPair(row -> new Tuple2<>(row._1(), statusTrue));
        new HBaseWrite().save(commentTableName, updateStatus, commentNetworkStatusColumn);
    }

}
