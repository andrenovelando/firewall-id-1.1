/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.firewallid.classification;

import com.firewallid.io.HDFSRead;
import com.firewallid.util.FIFile;
import com.firewallid.util.FIUtils;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.linalg.Vector;
import scala.Tuple2;

/**
 *
 * @author andrenovelando@gmail.com
 */
public class FeatureExtraction extends Classification {

    public static final String FEATUREINDICESPATH = "classification.featureextraction.featureindicespath";

    /* Constructor */
    public FeatureExtraction(JavaSparkContext javaSparkContext) {
        super(javaSparkContext);
    }

    public void saveFeatureIndices() throws IOException {
        String featureIndicesPath = conf.get(FEATUREINDICESPATH);
        int minDocFreq = conf.getInt(MIN_DOCFREQ, 1);

        FIFile.deleteExistHDFSPath(featureIndicesPath);

        trainingData
                /* One document have distinct feature */
                .map(line -> FIUtils.listToSet(extractTermList(line)))
                .flatMapToPair(FIUtils::setToTupleList)
                /* Return all feature with no duplicate */
                .reduceByKey((t1, t2) -> t1 + t2)
                /* Filter for no indices have two features or more */
                .filter(t -> t._2 >= minDocFreq)
                .mapToPair(t -> new Tuple2<Integer, String>(hashingTF.indexOf(t._1), t._1))
                .saveAsObjectFile(featureIndicesPath);
        LOG.info("FeatureExtraction: feature indices file saved to " + featureIndicesPath);
    }

    public Map<Integer, String> loadFeatureIndices() {
        String featureIndicesPath = conf.get(FEATUREINDICESPATH);

        JavaPairRDD<Integer, String> documents = new HDFSRead().loadObjFile(jsc, featureIndicesPath)
                .mapToPair(row -> {
                    Tuple2<Integer, String> rowCast = (Tuple2<Integer, String>) row;
                    return new Tuple2(rowCast._1, rowCast._2);
                });

        return documents.collectAsMap();
    }

    public JavaPairRDD<String, Vector> transformTFIDF(JavaPairRDD<String, List<String>> doc) {
        return doc.mapToPair(t -> new Tuple2(t._1, createVector(t._2)));
    }

    /* Combine two tfidf. Sum all vector on same key */
    public JavaPairRDD<String, Vector> combineTFIDF(JavaPairRDD<String, Vector> doc1, JavaPairRDD<String, Vector> doc2) {
        if (doc1 == null) {
            return doc2;
        } else if (doc2 == null) {
            return doc1;
        }

        /* Union */
        JavaPairRDD<String, Vector> union = doc1.union(doc2).reduceByKey((v1, v2) -> FIUtils.sumVector(v1, v2));

        return union;
    }

    /* Select Top N tfidf value */
    public JavaPairRDD<String, List<Tuple2<Integer, Double>>> selectTopTFIDF(JavaPairRDD<String, Vector> doc, int n) {
        JavaPairRDD topDoc = doc.mapToPair(t -> {
            List<Tuple2<Integer, Double>> list = FIUtils.vectorToList(t._2);
            List<Tuple2<Integer, Double>> sortedlist = FIUtils.sortDescTupleListByValue(list);
            List<Tuple2<Integer, Double>> nFirstList;
            if (sortedlist.size() <= n) {
                nFirstList = new ArrayList<>(sortedlist);
            } else {
                nFirstList = new ArrayList<>(sortedlist.subList(0, n));
            }
            return new Tuple2(t._1, nFirstList);
        });

        return topDoc;
    }

    /* Transform Indice to Feature */
    public JavaPairRDD<String, List<Tuple2<String, Double>>> transformIndiceToFeature(JavaPairRDD<String, List<Tuple2<Integer, Double>>> doc) {
        /* Load indicices-feature */
        Map<Integer, String> indices_feature = loadFeatureIndices();

        JavaPairRDD<String, List<Tuple2<String, Double>>> transformDoc = doc.mapToPair(t -> {
            List<Tuple2<String, Double>> features = t._2.parallelStream()
                    .map(t1 -> new Tuple2<String, Double>(indices_feature.get(t1._1), t1._2))
                    .filter(t1 -> t1._1 != null)
                    .collect(Collectors.toList());

            return new Tuple2(t._1, features);
        });

        return transformDoc;
    }
}
