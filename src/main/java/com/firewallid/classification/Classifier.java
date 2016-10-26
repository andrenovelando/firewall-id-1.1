/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.firewallid.classification;

/**
 *
 * @author andrenovelando@gmail.com
 */
import com.firewallid.io.HBaseRead;
import com.firewallid.io.HBaseTableUtils;
import com.firewallid.io.HBaseWrite;
import com.firewallid.util.FIConfiguration;
import com.firewallid.util.FIFile;
import com.google.common.base.Joiner;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.mllib.classification.SVMModel;
import org.apache.spark.mllib.linalg.Vector;
import scala.Tuple2;

public class Classifier extends Classification {

    public static final String STATUSTRUE = "firewallindonesia.status.true";
    public static final String NAME_DELIMITER = "firewallindonesia.name.delimiter";

    public static final String NONELABEL = "classification.classifier.labels.none";

    public static final String CRAWL_SUFFIXNAME = "table.crawl.suffixname";
    public static final String CLEANING_COLUMN = "table.crawl.cleaningcolumn";
    public static final String CLASSIFICATION_COLUMN = "table.crawl.classificationcolumn";
    public static final String CLASSIFIED_COLUMN = "table.crawl.classifiedcolumn";

    List<Tuple2<String, SVMModel>> models;

    public Classifier(JavaSparkContext jsc) {
        super(jsc);
        models = loadModels();
    }

    private List<Tuple2<String, SVMModel>> loadModels() {
        String modelpath = conf.get(MODEL_PATH);

        List<Tuple2<String, SVMModel>> modelsLoad = labels.parallelStream()
                .map(label -> new Tuple2<String, SVMModel>(label, SVMModel.load(jsc.sc(), FIFile.generateFullPath(modelpath, label))))
                .collect(Collectors.toList());

        return modelsLoad;
    }

    public String classify(String cleanedText) {
        String noneLabel = conf.get(NONELABEL);
        String textDelimiter = conf.get(TEXT_DELIMITER);

        if (cleanedText.isEmpty()) {
            return noneLabel;
        }

        Vector dataVector = createVector(cleanedText.split(textDelimiter));
        List<String> categories = models.parallelStream()
                .filter(model -> model._2.predict(dataVector) >= 1)
                .map(model -> model._1())
                .collect(Collectors.toList());

        if (categories.isEmpty()) {
            return noneLabel;
        }

        String category = Joiner.on(textDelimiter).join(categories);

        return category;
    }

    public JavaPairRDD<String, String> classify(JavaPairRDD<String, String> notClassifiedRow) {
        /* Classification */
        JavaPairRDD<String, String> classifiedRow = notClassifiedRow.mapToPair(row -> {
            String categories = classify(row._2());
            LOG.info(String.format("Key: %s. Label: %s", row._1(), categories));
            return new Tuple2<>(row._1(), categories);
        });

        return classifiedRow;
    }

    /* Run on spark */
    public void run(String type, String prefixTableName) throws IOException {
        conf.addResource(type + "-default.xml");

        String tableName = prefixTableName + conf.get(NAME_DELIMITER) + conf.get(CRAWL_SUFFIXNAME);
        String cleaningColumn = conf.get(CLEANING_COLUMN);
        String classificationColumn = conf.get(CLASSIFICATION_COLUMN);
        String classifiedColumn = conf.get(CLASSIFIED_COLUMN);
        String noneLabel = conf.get(NONELABEL);
        String statusTrue = conf.get(STATUSTRUE);

        /* Read */
        JavaPairRDD<String, String> inClassification = new HBaseRead().doRead(jsc, tableName, cleaningColumn, classificationColumn);

        /* Classifier */
        JavaPairRDD<String, Map<String, String>> outClassification = classify(inClassification)
                .mapToPair(row -> {
                    Map<String, String> map = new HashMap<>();
                    map.put(classificationColumn, row._2());
                    if (!row._2().equals(noneLabel)) {
                        map.put(classifiedColumn, statusTrue);
                    }
                    return new Tuple2<>(row._1(), map);
                });

        /* Save */
        List<String> family = Arrays.asList(HBaseTableUtils.getFamily(classificationColumn), HBaseTableUtils.getFamily(classifiedColumn));
        new HBaseWrite().save(tableName, outClassification, family);
    }

    public static void main(String[] args) throws IOException {
        if (args.length < 2 || !Arrays.asList("website", "twitter", "facebook").contains(args[0])) {
            System.out.println("Arguments: website|twitter|facebook <tableId>");
            System.exit(-1);
        }

        String type = args[0];
        String prefixTableName = args[1];

        JavaSparkContext javaSparkContext = new JavaSparkContext(FIConfiguration.createSparkConf(null));

        new Classifier(javaSparkContext).run(prefixTableName, type);
    }
}
