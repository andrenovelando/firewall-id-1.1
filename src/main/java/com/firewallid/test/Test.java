package com.firewallid.test;

///*
// * To change this license header, choose License Headers in Project Properties.
// * To change this template file, choose Tools | Templates
// * and open the template in the editor.
// */
//package com.firewallindonesia.test;
//
//import com.firewallindonesia.classification.Classification;
//import com.firewallindonesia.classification.Classifier;
//import com.firewallindonesia.classification.FeatureExtraction;
//import com.firewallindonesia.cleaning.IDFormalization;
//import com.firewallindonesia.cleaning.IDRemoval;
//import static com.firewallindonesia.count.Count.CLASSIFICATION_TABLE;
//import static com.firewallindonesia.count.Count.DB_ADDRESS;
//import static com.firewallindonesia.count.Count.DB_PASSWORD;
//import static com.firewallindonesia.count.Count.DB_USERNAME;
//import static com.firewallindonesia.count.Count.DRIVER_CLASS;
//import com.firewallindonesia.datapreparation.IDLanguageDetector;
//import com.firewallindonesia.io.HBaseRead;
//import com.firewallindonesia.io.HBaseWrite;
//import com.firewallindonesia.termcloud.TermCloudBuilder;
//import com.firewallindonesia.util.FIConfiguration;
//import com.firewallindonesia.util.FIFile;
//import java.io.IOException;
//import java.sql.PreparedStatement;
//import java.sql.ResultSet;
//import java.sql.SQLException;
//import org.apache.spark.rdd.JdbcRDD;
//import java.util.ArrayList;
//import java.util.Arrays;
//import java.util.HashMap;
//import java.util.Iterator;
//import java.util.List;
//import java.util.Map;
//import java.util.Properties;
//import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.fs.FileSystem;
//import org.apache.hadoop.fs.FileUtil;
//import org.apache.hadoop.fs.Path;
//import org.apache.hadoop.hbase.client.Result;
//import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
//import org.apache.hadoop.hbase.util.Bytes;
//import org.apache.hadoop.mapred.JobConf;
//import org.apache.hadoop.mapred.lib.db.DBConfiguration;
//import org.apache.hadoop.mapred.lib.db.DBOutputFormat;
//import org.apache.hadoop.mapred.lib.db.DBWritable;
//import org.apache.spark.SparkConf;
//import org.apache.spark.api.java.JavaPairRDD;
//import org.apache.spark.api.java.JavaRDD;
//import org.apache.spark.api.java.JavaSparkContext;
//import org.apache.spark.api.java.function.Function;
//import org.apache.spark.api.java.function.VoidFunction;
//import org.apache.spark.mllib.feature.HashingTF;
//import org.apache.spark.mllib.linalg.Vector;
//import org.apache.spark.sql.DataFrame;
//import org.apache.spark.sql.Row;
//import org.apache.spark.sql.RowFactory;
//import org.apache.spark.sql.SQLContext;
//import org.apache.spark.sql.SaveMode;
//import org.apache.spark.sql.types.DataTypes;
//import org.apache.spark.sql.types.StructField;
//import org.apache.spark.sql.types.StructType;
//import org.apache.spark.storage.StorageLevel;
//import scala.Function0;
//import scala.Function1;
//import scala.Int;
//import scala.Tuple2;
//import scala.collection.JavaConverters;
////import scala.collection.mutable;
////import scala.collection.mutable.HashMap;
//
///**
// *
// * @author andrenovelando@gmail.com
// */
//public class Test {
//
//    static Map<String, String> termFrequencies = new HashMap<>();
//static IDLanguageDetector idLanguageDetector = new IDLanguageDetector();
//    public static void main(String[] args) {
//        JavaSparkContext javaSparkContext = new JavaSparkContext(FIConfiguration.createSparkConf(null));
//        List<String> list = new ArrayList<>();
//        list.add("saya pergi ke pasar");
//        list.add("saya pergi ke pasar");
//        list.add("saya pergi ke pasar");
//        list.add("");
//        list.add("");
//        list.add("");
//        list.add("");
//        list.add("");
//        list.add("");
//        list.add("");
//        list.add("");
//        list.add("");
//        list.add("");
//        list.add("");
//        list.add("");
//        list.add("");
//        list.add("");
//        long count = javaSparkContext.parallelize(list).filter((String s) -> idLanguageDetector.detect(s)).count();
//        System.out.println("COUNT " + count);
//    }
//}
