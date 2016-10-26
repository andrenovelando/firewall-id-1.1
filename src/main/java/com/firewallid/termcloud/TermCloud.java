/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.firewallid.termcloud;

import com.firewallid.base.Base;
import com.firewallid.util.FIFile;
import com.firewallid.util.FIUtils;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.commons.lang.StringEscapeUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;

/**
 *
 * @author andrenovelando@gmail.com
 */
public class TermCloud extends Base {

    public static final String PARTITIONS = "firewallindonesia.read.partitions";
    public static final String NAME_DELIMITER = "firewallindonesia.name.delimiter";
    public static final String LINE_DELIMITER = "firewallindonesia.file.linedelimiter";
    public static final String TEXT_DELIMITER = "firewallindonesia.file.textdelimiter";

    public static final String TITLETFIDF_FOLDER = "termcloud.titletfidf.folder";
    public static final String TERMCLOUD_FOLDER = "termcloud.output.folder";
    public static final String TERMCLOUD_SUFFIXNAME = "termcloud.output.file.suffixname";
    public static final String ALLNAME = "termcloud.output.file.allname";
    public static final String TOPN = "termcloud.output.topn";

    /* Constructor */
    public TermCloud() {
    }

    /* Input: title-text. Return one titleName -> one list of feature */
    public JavaPairRDD<String, List<String>> reduceByTitle(JavaPairRDD<String, String> doc) {
        JavaPairRDD<String, List<String>> titleText = doc
                .mapToPair(data -> new Tuple2<String, List<String>>(data._1, Arrays.asList(data._2.split(conf.get(TEXT_DELIMITER)))))
                /* One row maybe have two or more title. Making it one-to-one correspondence*/
                .flatMapToPair(titleFeatures -> Arrays.asList(titleFeatures._1.split(conf.get(TEXT_DELIMITER))).parallelStream().map(s -> new Tuple2<String, List<String>>(s, titleFeatures._2)).collect(Collectors.toList()))
                .reduceByKey(FIUtils::combineList)
                .persist(StorageLevel.MEMORY_AND_DISK_SER_2())
                .repartition(conf.getInt(PARTITIONS, 48));

        return titleText;
    }

    /* save titleName-vector tfidf pair */
    public void saveTitleTFIDF(JavaPairRDD<String, Vector> save, String fileName) throws IOException {
        String outPath = FIFile.generateFullPath(conf.get(TITLETFIDF_FOLDER), fileName);
        FIFile.deleteExistHDFSPath(outPath);
        save.saveAsObjectFile(outPath);
    }

    /* Return true if title-tfidf file exists */
    public boolean isExistsTitleTFIDF(String fileName) throws IOException {
        String inPath = FIFile.generateFullPath(conf.get(TITLETFIDF_FOLDER), fileName);
        return FIFile.isExistsHDFSPath(inPath);
    }

    /* load titleName-vector tfidf pair from previously saved objectfile */
    public JavaPairRDD<String, Vector> loadTitleTFIDF(JavaSparkContext jsc, String fileName) throws IOException {
        if (!isExistsTitleTFIDF(fileName)) {
            return null;
        }

        String inPath = FIFile.generateFullPath(conf.get(TITLETFIDF_FOLDER), fileName);

        JavaRDD<Tuple2<String, Vector>> loadRDD = jsc.objectFile(inPath);
        JavaPairRDD<String, Vector> loadPair = loadRDD.persist(StorageLevel.MEMORY_AND_DISK_SER_2())
                .repartition(conf.getInt(PARTITIONS, 48))
                .mapToPair((Tuple2<String, Vector> t) -> new Tuple2(t._1, t._2));

        return loadPair;
    }

    /* Return formatted file name termcloud */
    public String createFileNameTermCloud(String prefix, String name) {
        return prefix + conf.get(NAME_DELIMITER) + name + conf.get(NAME_DELIMITER) + conf.get(TERMCLOUD_SUFFIXNAME);
    }

    /* Each titleName have one file in hdfs. No duplicate titleName (key) input */
    public void saveTermCloud(JavaPairRDD<String, List<Tuple2<String, Double>>> doc, String fileNamePrefix) {
        doc
                .filter(titleFeatures -> !titleFeatures._2.isEmpty())
                /* Map list to formatted line of termcloud file */
                .mapToPair(titleFeatures
                        -> new Tuple2<String, String>(titleFeatures._1,
                                titleFeatures._2
                                .parallelStream()
                                .map(feature
                                        -> feature._1 + StringEscapeUtils.unescapeJava(conf.get(LINE_DELIMITER)) + feature._2)
                                .collect(Collectors.joining(System.lineSeparator()))))
                /* Save to hdfs file */
                .foreach(titleText -> FIFile.writeStringToHDFSFile(FIFile.generateFullPath(conf.get(TERMCLOUD_FOLDER), createFileNameTermCloud(fileNamePrefix, titleText._1)), titleText._2));
    }

    /* Combine all title's termCloud into one file */
    public void saveTermCloudAll(JavaPairRDD<String, List<Tuple2<String, Double>>> doc, String fileNamePrefix) throws IOException {
        List<Tuple2<String, List<Tuple2<String, Double>>>> collectDoc = doc.collect();

        if (collectDoc.isEmpty()) {
            return;
        }

        /* Reduced feature-value list */
        List<Tuple2<String, Double>> featureValueList = collectDoc.parallelStream()
                .map(titleFeatures -> titleFeatures._2)
                .reduce((featureValueList1, featureValueList2) -> {
                    List<Tuple2<String, Double>> combineList = FIUtils.combineList(featureValueList1, featureValueList2);

                    List<Tuple2<String, Double>> collect = combineList.parallelStream()
                            .collect(Collectors.groupingBy(t -> t._1, Collectors.mapping(t -> t._2, Collectors.toList())))
                            .entrySet().parallelStream().map(t -> new Tuple2<String, Double>(t.getKey(), t.getValue().parallelStream().mapToDouble(Double::doubleValue).sum()))
                            .collect(Collectors.toList());

                    return collect;
                }).get();

        /* Sorting */
        List<Tuple2<String, Double>> featureValueListSorted = FIUtils.sortDescTupleListByValue(featureValueList);

        /* Top N */
        List<Tuple2<String, Double>> featureValueListTopN;
        if (featureValueListSorted.size() <= conf.getInt(TOPN, 100)) {
            featureValueListTopN = new ArrayList<>(featureValueListSorted);
        } else {
            featureValueListTopN = new ArrayList<>(featureValueListSorted.subList(0, conf.getInt(TOPN, 100)));
        }

        /* Text for file. One line, one feature-value pair */
        String featureValueText = featureValueListTopN.parallelStream().map(feature -> feature._1 + StringEscapeUtils.unescapeJava(conf.get(LINE_DELIMITER)) + feature._2).collect(Collectors.joining(System.lineSeparator()));

        /* Save to file */
        FIFile.writeStringToHDFSFile(FIFile.generateFullPath(conf.get(TERMCLOUD_FOLDER), createFileNameTermCloud(fileNamePrefix, conf.get(ALLNAME))), featureValueText);
    }
}
