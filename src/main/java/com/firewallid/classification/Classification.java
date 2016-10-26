/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.firewallid.classification;

import com.firewallid.base.Base;
import com.firewallid.io.HDFSRead;
import java.util.Arrays;
import java.util.List;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.feature.HashingTF;
import org.apache.spark.mllib.feature.IDF;
import org.apache.spark.mllib.feature.IDFModel;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.regression.LabeledPoint;

/**
 *
 * @author andrenovelando@gmail.com
 */
public class Classification extends Base {

    public static final String TEXT_DELIMITER = "firewallindonesia.file.textdelimiter";
    public static final String DOC_DELIMITER = "firewallindonesia.file.linedelimiter";

    public static final String DATA_POSITION = "classification.trainingdata.dataposition";
    public static final String LABEL_POSITION = "classification.trainingdata.labelposition";
    public static final String TRAININGDATA_PATH = "classification.trainingdata.path";
    public static final String MIN_DOCFREQ = "classification.idf.mindocfreq";
    public static final String LABELS = "classification.classifier.labels";
    public static final String MODEL_PATH = "classification.classifier.modelpath";

    static JavaSparkContext jsc;
    List<String> labels;
    JavaRDD<String> trainingData;
    HashingTF hashingTF;
    IDFModel idf;

    public Classification(JavaSparkContext jsc) {
        Classification.jsc = jsc;
        labels = Arrays.asList(conf.getStrings(LABELS));
        trainingData = new HDFSRead().loadTextFile(jsc, conf.get(TRAININGDATA_PATH));
        hashingTF = new HashingTF();
        idf = createIDF(trainingData);
    }

    public String extractLabel(String line) {
        String docDelimiter = conf.get(DOC_DELIMITER);
        int labelPosition = conf.getInt(LABELS, 1);

        return line.split(docDelimiter)[labelPosition - 1];
    }

    public List<String> extractTermList(String line) {
        if (line.isEmpty()) {
            return null;
        }

        String docDelimiter = conf.get(DOC_DELIMITER);
        int dataPosition = conf.getInt(DATA_POSITION, 3);
        String textDelimiter = conf.get(TEXT_DELIMITER);
        String terms = line.split(docDelimiter)[dataPosition - 1];
        List<String> termList = Arrays.asList(terms.split(textDelimiter));
        termList.removeAll(Arrays.asList(""));

        if (termList.isEmpty()) {
            return null;
        }

        return termList;
    }

    /* Return vector using TF-IDF */
    public Vector createVector(String line) {
        List<String> data = extractTermList(line);
        Vector dataVector = idf.transform(hashingTF.transform(data));

        return dataVector;
    }

    public Vector createVector(List<String> terms) {
        Vector dataVector = idf.transform(hashingTF.transform(terms));

        return dataVector;
    }

    public Vector createVector(String[] terms) {
        List<String> data = Arrays.asList(terms);

        return createVector(data);
    }

    public LabeledPoint createLabeledPoint(String line, String label) {
        Vector dataVector = createVector(line);
        String dataLabel = extractLabel(line);
        if (label.equals(dataLabel.toLowerCase())) {
            return new LabeledPoint(1.0, dataVector);
        } else {
            return new LabeledPoint(0.0, dataVector);
        }
    }

    public final IDFModel createIDF(JavaRDD<String> doc) {
        /* Load file as documents */
        JavaRDD<List<String>> documents = doc.map(line -> extractTermList(line));

        /* Create IDF */
        int minDocFreq = conf.getInt(MIN_DOCFREQ, 1);
        JavaRDD tf = hashingTF.transform(documents);
        IDFModel idfModel = new IDF(minDocFreq).fit(tf);
        LOG.info("IDF: Created");

        return idfModel;
    }
}
