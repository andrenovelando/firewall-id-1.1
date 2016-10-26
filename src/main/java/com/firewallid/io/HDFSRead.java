/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.firewallid.io;

import com.firewallid.base.Base;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;

/**
 *
 * @author andrenovelando@gmail.com
 */
public class HDFSRead extends Base {

    public static final String PARTITIONS = "firewallindonesia.read.partitions";

    public HDFSRead() {
        super();
    }

    public JavaRDD<String> loadTextFile(JavaSparkContext jsc, String path) {
        int partitions = conf.getInt(PARTITIONS, 48);

        return jsc.textFile(path)
                .persist(StorageLevel.MEMORY_AND_DISK_SER_2())
                .repartition(partitions);
    }

    public JavaRDD<Object> loadObjFile(JavaSparkContext jsc, String path) {
        int partitions = conf.getInt(PARTITIONS, 48);

        return jsc.objectFile(path)
                .persist(StorageLevel.MEMORY_AND_DISK_SER_2())
                .repartition(partitions);
    }
}
