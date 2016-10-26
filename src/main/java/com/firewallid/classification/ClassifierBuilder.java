/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.firewallid.classification;

import com.firewallid.util.FIConfiguration;
import com.firewallid.util.FIFile;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.classification.SVMModel;
import org.apache.spark.mllib.classification.SVMWithSGD;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.rdd.RDD;

/**
 *
 * @author andrenovelando@gmail.com
 */
public class ClassifierBuilder extends Classification {

    public static final String ITERATIONS = "classification.svm.iterations";

    public ClassifierBuilder(JavaSparkContext jsc) {
        super(jsc);
    }

    public SVMModel createSVMModel(String label) {
        int iterations = conf.getInt(ITERATIONS, 100);

        /* Documents to LabeledPoint */
        RDD<LabeledPoint> trainingdata = trainingData.map(line -> createLabeledPoint(line, label)).rdd();

        /* Create model */
        SVMModel model = SVMWithSGD.train(trainingdata, iterations);
        model.clearThreshold();
        LOG.info(label + " SVMModel created");

        /* Save model */
        String modelpath = conf.get(MODEL_PATH);
        model.save(jsc.sc(), FIFile.generateFullPath(modelpath, label));
        LOG.info(label + " SVMModel saved");

        return model;
    }

    public void run() {
        labels.stream().forEach(label -> createSVMModel(label));
    }

    public static void main(String[] args) {
        /*Inisiasi JavaSparkContext*/
        JavaSparkContext javaSparkContext = new JavaSparkContext(FIConfiguration.createSparkConf(null));

        /* Build classifier */
        new ClassifierBuilder(javaSparkContext).run();
    }
}
