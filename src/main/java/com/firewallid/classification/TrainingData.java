/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.firewallid.classification;

import com.firewallid.base.Base;
import com.firewallid.cleaning.Cleaning;
import com.firewallid.datapreparation.IDLanguageDetector;
import com.firewallid.io.HDFSRead;
import com.firewallid.util.FIConnection;
import com.firewallid.util.FIFile;
import java.io.IOException;
import java.util.List;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringEscapeUtils;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;

/**
 *
 * @author andrenovelando@gmail.com
 */
public class TrainingData extends Base {

    public static final String PARTITIONS = "firewallindonesia.read.partitions";
    public static final String DOC_DELIMITER = "firewallindonesia.file.linedelimiter";

    public static final String TIMEOUT = "classification.trainingdata.timeoutdownload";
    public static final String TRAININGDATA_PATH = "classification.trainingdata.path";
    public static final String TRAININGDATA_SOURCE = "classification.trainingdata.source";
    public static final String TRAININGDATA_RESOURCE = "classification.trainingdata.resource";

    static IDLanguageDetector idLanguageDetector = new IDLanguageDetector();
    static Cleaning cleaning = new Cleaning();

    public TrainingData() {
        super();
    }

    /* Load text document from resource. Line format: label(docDelimiter)url */
    public JavaPairRDD<String, String> load(JavaSparkContext jsc) throws IOException {
        int partitions = conf.getInt(PARTITIONS, 48);
        String docDelimiter = StringEscapeUtils.unescapeJava(conf.get(DOC_DELIMITER));

        List<String> resource = IOUtils
                .readLines(getClass().getClassLoader().getResourceAsStream(conf.get(TRAININGDATA_SOURCE)));

        Broadcast<List<String>> broadcastRes = jsc.broadcast(resource);

        JavaPairRDD<String, String> doc = jsc
                .parallelize(broadcastRes.value())
                .persist(StorageLevel.MEMORY_AND_DISK_SER_2())
                .repartition(partitions)
                .filter(line -> line.split(docDelimiter).length >= 2)
                .mapToPair(line -> new Tuple2<>(line.split(docDelimiter)[0], line.split(docDelimiter)[1]));

        return doc;
    }

    /* Download text from url. Input: (label -> url). Output (label -> (url, text)) */
    public JavaPairRDD<String, Tuple2<String, String>> download(JavaPairRDD<String, String> doc) {
        int timeout = conf.getInt(TIMEOUT, 60);

        JavaPairRDD<String, Tuple2<String, String>> download = doc
                .mapToPair(row -> new Tuple2<>(row._1(),
                        new Tuple2<>(row._2(), FIConnection.getText(row._2(), timeout))))
                .filter(row -> !row._2()._2().isEmpty());

        return download;
    }

    /* Filter indonesian language text. Cleaning the text. Input (label -> (url, text)). Input (label -> (url, cleaned_text)) */
    public JavaPairRDD<String, Tuple2<String, String>> clean(JavaPairRDD<String, Tuple2<String, String>> doc) {
        JavaPairRDD<String, Tuple2<String, String>> clean = doc
                .filter(row -> idLanguageDetector.detect(row._2()._2()))
                .mapToPair(row -> new Tuple2<>(row._1(),
                        new Tuple2<>(row._2()._1(), cleaning.textCleaningDelimiter(row._2()._2()))));

        return clean;
    }

    /* Save as text document. Line format url(docDelimiter)label(docDelimiter)cleaned_text */
    public void save(JavaPairRDD<String, Tuple2<String, String>> doc) throws IOException {
        String docDelimiter = StringEscapeUtils.unescapeJava(conf.get(DOC_DELIMITER));
        String dataTrainingPath = conf.get(TRAININGDATA_PATH);
        String dataTrainingPathTemp = dataTrainingPath + "-tmp";

        FIFile.deleteExistHDFSPath(dataTrainingPath);
        FIFile.deleteExistHDFSPath(dataTrainingPathTemp);

        doc
                .map(row -> row._1() + docDelimiter + row._2()._1() + docDelimiter + row._2()._2())
                .saveAsTextFile(dataTrainingPathTemp);

        FIFile.copyMergeDeleteHDFS(dataTrainingPathTemp, dataTrainingPath);
    }

    /* Upload training data from resource path to hdfs */
    public void upload(JavaSparkContext jsc) throws IOException {
        int partitions = conf.getInt(PARTITIONS, 48);
        String docDelimiter = StringEscapeUtils.unescapeJava(conf.get(DOC_DELIMITER));

        /* load training data */
        List<String> resource = IOUtils
                .readLines(getClass().getClassLoader().getResourceAsStream(conf.get(TRAININGDATA_RESOURCE)));

        Broadcast<List<String>> broadcastRes = jsc.broadcast(resource);

        JavaPairRDD<String, Tuple2<String, String>> doc = jsc
                .parallelize(broadcastRes.value())
                .persist(StorageLevel.MEMORY_AND_DISK_SER_2())
                .repartition(partitions)
                .filter(line -> line.split(docDelimiter).length >= 2)
                .mapToPair(line -> new Tuple2<>(line.split(docDelimiter)[0],
                        new Tuple2<>(line.split(docDelimiter)[1], line.split(docDelimiter)[2])));
        /* save */
        save(doc);
    }

    /* Run on spark. Condition: null -> upload. empty -> download (default path). not empty -> download (custom path) */
    public void run(JavaSparkContext jsc, String trainingDataPath) throws IOException {
        if (trainingDataPath == null) {
            upload(jsc);
            return;
        }

        JavaPairRDD<String, String> docLoad;
        if (trainingDataPath.isEmpty()) {
            docLoad = load(jsc);
        } else {
            String docDelimiter = StringEscapeUtils.unescapeJava(conf.get(DOC_DELIMITER));
            docLoad = new HDFSRead().loadTextFile(jsc, trainingDataPath).mapToPair(line -> new Tuple2<>(line.split(docDelimiter)[0], line.split(docDelimiter)[1]));

        }

        JavaPairRDD<String, Tuple2<String, String>> docDownload = download(docLoad);
        JavaPairRDD<String, Tuple2<String, String>> docClean = clean(docDownload);
        save(docClean);
    }
}
