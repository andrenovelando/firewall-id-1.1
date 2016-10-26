/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.firewallid.io;

import com.firewallid.base.Base;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapred.TableOutputFormat;
import org.apache.hadoop.hbase.regionserver.NoSuchColumnFamilyException;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapred.JobConf;
import org.apache.spark.api.java.JavaPairRDD;
import scala.Tuple2;

/**
 *
 * @author andrenovelando@gmail.com
 */
public class HBaseWrite extends Base{
    
    public static final String TMP_FILE = "hbaseinterface.read.tmp.file";
    
    public Tuple2<ImmutableBytesWritable, Put> convertRowToPut (String rowId, String column, String value) {
        Put put = new Put(Bytes.toBytes(rowId));
        if (column != null && value != null) {
            put.add(Bytes.toBytes(column.split(":")[0]),
                    Bytes.toBytes(column.split(":")[1]),
                    Bytes.toBytes(value));
        }
        
        if (put.isEmpty()) {
            return null;
        }
        
        return new Tuple2(new  ImmutableBytesWritable(), put);
    }
    
    public Tuple2<ImmutableBytesWritable, Put> convertRowToPut (Tuple2<String, Map<String, String>> row) {
        Put put = new Put(Bytes.toBytes(row._1));
        row._2.entrySet().stream().forEach((columnValue) -> {
            String column = columnValue.getKey();
            String value = columnValue.getValue();
            if (column != null && value != null) {
                put.add(Bytes.toBytes(column.split(":")[0]),
                        Bytes.toBytes(column.split(":")[1]),
                        Bytes.toBytes(value));
            }
        });
        
        if (put.isEmpty()) {
            return null;
        }
        
        return new Tuple2(new  ImmutableBytesWritable(), put);
    }
    
    public void save (String tableName, JavaPairRDD<String, String> savePairRDD, String destColumn) throws IOException {
        /* Check hbase table */
        if (!HBaseTableUtils.istableExists(tableName)) {
            throw new TableNotFoundException();
        }
        
        /* Check column family */
        if (!HBaseTableUtils.isFamilyExists(tableName, destColumn.split(":")[0])) {
            throw new NoSuchColumnFamilyException();
        }
        
        /* Save to HBase */
        JobConf jobConf = new JobConf();
        jobConf.setOutputFormat(TableOutputFormat.class);
        jobConf.set(TableOutputFormat.OUTPUT_TABLE, tableName);
        
        savePairRDD
                .mapToPair((Tuple2<String, String> t) -> 
                convertRowToPut(t._1, destColumn, t._2))
                .filter((Tuple2<ImmutableBytesWritable, Put> t1) -> t1 != null)
                .saveAsHadoopDataset(jobConf);
    }
    
    public void save (String tableName, JavaPairRDD<String, Map<String, String>> savePairRDD, List<String> destFamilys) throws IOException {
        /* Check hbase table */
        if (!HBaseTableUtils.istableExists(tableName)) {
            throw new TableNotFoundException();
        }
        
        /* Check column family */
        if (!HBaseTableUtils.isFamilyExists(tableName, destFamilys)) {
            throw new NoSuchColumnFamilyException();
        }
        
        /* Save to HBase */
        JobConf jobConf = new JobConf();
        jobConf.setOutputFormat(TableOutputFormat.class);
        jobConf.set(TableOutputFormat.OUTPUT_TABLE, tableName);
        
        savePairRDD
                .mapToPair((Tuple2<String, Map<String, String>> t) -> convertRowToPut(t))
                .filter((Tuple2<ImmutableBytesWritable, Put> t1) -> t1 != null)
                .saveAsHadoopDataset(jobConf);
    }
}
