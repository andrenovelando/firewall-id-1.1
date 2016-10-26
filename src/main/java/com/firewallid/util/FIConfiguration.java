/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.firewallid.util;

import com.firewallid.io.HBaseRead;
import com.firewallid.io.HBaseWrite;
import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.SparkConf;

/**
 *
 * @author andrenovelando@gmail.com
 */
public class FIConfiguration {
    
    private static Configuration addFirewallIndonesiaResources(Configuration conf) {
        conf.addResource("firewallid-default.xml");
        return conf;
    }
    
    public static Configuration create() {
        Configuration conf = new Configuration();
        conf.clear();
        addFirewallIndonesiaResources(conf);
        return conf;
    }
    
    public static SparkConf createSparkConf(Class[] clazz){
        Class[] defaultClass = {FIConfiguration.class, HBaseRead.class, HBaseWrite.class, Configuration.class, Result.class, ImmutableBytesWritable.class, Bytes.class};
        
        SparkConf sparkConf = new SparkConf();
        sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        sparkConf.registerKryoClasses((Class[]) ArrayUtils.addAll(defaultClass, clazz));
        return sparkConf;
    }
}
