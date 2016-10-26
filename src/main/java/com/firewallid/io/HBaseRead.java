/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.firewallid.io;

import com.firewallid.base.Base;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.regionserver.NoSuchColumnFamilyException;
import org.apache.hadoop.hbase.util.Base64;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;

/**
 *
 * @author andrenovelando@gmail.com
 */
public class HBaseRead extends Base {

    public static final String PARTITIONS = "firewallindonesia.read.partitions";

    public static final String CACHE_BLOCKS = "hbaseinterface.read.cacheblocks";
    public static final String CACHING = "hbaseinterface.read.caching";

    public HBaseRead() {
    }

    public String getRowId(Result r) {
        return Bytes.toString(r.getRow());
    }

    public String getValue(Result r, String column) {
        return Bytes.toString(r.getValue(Bytes.toBytes(column.split(":")[0]), Bytes.toBytes(column.split(":")[1])));
    }

    /* Return row key-column value pair */
    public Tuple2<String, String> getRowResult(Tuple2<ImmutableBytesWritable, Result> t, String column) {
        String rowId = getRowId(t._2);
        String value = getValue(t._2, column);

        if (value == null || rowId == null) {
            return null;
        }

        return new Tuple2(rowId, value);
    }

    /* Return row key-column value map pair */
    public Tuple2<String, Map<String, String>> getRowResult(Tuple2<ImmutableBytesWritable, Result> t, List<String> resultColumns) {
        String rowId = getRowId(t._2);
        List<Tuple2<String, String>> columnValueList = resultColumns.parallelStream().map(column -> getRowResult(t, column)).collect(Collectors.toList());

        if (columnValueList.contains(null) || rowId == null) {
            return null;
        }

        Map<String, String> columnValueMap = columnValueList.parallelStream()
                .collect(Collectors.toMap(tuple2 -> tuple2._1, tuple2 -> tuple2._2));

        return new Tuple2(rowId, columnValueMap);
    }

    public String convertScanToString(Scan scan) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(out);
        scan.write(dos);
        return Base64.encodeBytes(out.toByteArray());
    }

    public JavaPairRDD<ImmutableBytesWritable, Result> scan(JavaSparkContext jsc, String tableName, List<String> existColumns, List<String> notexistColumns) throws IOException {
        /* Check hbase table */
        if (!HBaseTableUtils.istableExists(tableName)) {
            throw new TableNotFoundException();
        }

        /* Check column family */
        List<String> familys = new ArrayList<>();
        existColumns.parallelStream().forEach((existColumn) -> {
            familys.add(existColumn.split(":")[0]);
        });
        if (notexistColumns != null) {
            notexistColumns.parallelStream().forEach((notexistColumn) -> {
                familys.add(notexistColumn.split(":")[0]);
            });
        }
        if (!HBaseTableUtils.isFamilyExists(tableName, familys)) {
            throw new NoSuchColumnFamilyException();
        }

        /* Scan configuration */
        Scan scan = new Scan();
        scan.setCacheBlocks(conf.getBoolean(CACHE_BLOCKS, false));
        scan.setCaching(conf.getInt(CACHING, 10000));

        /* Scan column */
        existColumns.stream().forEach((existColumn) -> {
            scan.addColumn(Bytes.toBytes(existColumn.split(":")[0]), Bytes.toBytes(existColumn.split(":")[1]));
        });
        if (notexistColumns != null) {
            notexistColumns.stream().forEach((notexistColumn) -> {
                scan.addColumn(Bytes.toBytes(notexistColumn.split(":")[0]), Bytes.toBytes(notexistColumn.split(":")[1]));
            });
        }

        /* Scan filter */
        if (notexistColumns != null) {
            FilterList filters = new FilterList();
            notexistColumns.stream().map((notexistColumn) -> new SingleColumnValueFilter(
                    Bytes.toBytes(notexistColumn.split(":")[0]),
                    Bytes.toBytes(notexistColumn.split(":")[1]),
                    CompareFilter.CompareOp.EQUAL, Bytes.toBytes("DOESNOTEXIST"))).forEach((filter) -> {
                filters.addFilter(filter);
            });
            scan.setFilter(filters);
        }

        Configuration hbaseConf = HBaseConfiguration.create();
        hbaseConf.set(TableInputFormat.INPUT_TABLE, tableName);
        hbaseConf.set(TableInputFormat.SCAN, convertScanToString(scan));

        JavaPairRDD<ImmutableBytesWritable, Result> result = jsc.newAPIHadoopRDD(hbaseConf, TableInputFormat.class, ImmutableBytesWritable.class, Result.class);

        return result;
    }

    public JavaPairRDD<String, String> doRead(JavaSparkContext jsc, String tableName, String existColumn, String notexistColumn) throws IOException {
        List<String> existColumns = new ArrayList<>();
        existColumns.add(existColumn);
        List<String> notexistColumns = new ArrayList<>();
        if (notexistColumn != null) {
            notexistColumns.add(notexistColumn);
        } else {
            notexistColumns = null;
        }

        JavaPairRDD<String, String> result = scan(jsc, tableName, existColumns, notexistColumns)
                .mapToPair(row -> getRowResult(row, existColumn))
                .filter(row -> row != null)
                .persist(StorageLevel.MEMORY_AND_DISK_SER_2())
                .repartition(conf.getInt(PARTITIONS, 48));

        return result;
    }

    public JavaPairRDD<String, Map<String, String>> doRead(JavaSparkContext jsc, String tableName, String existColumn, String notexistColumn, List<String> resultColumns) throws IOException {
        List<String> existColumns = new ArrayList<>();
        existColumns.add(existColumn);
        existColumns.addAll(resultColumns);

        List<String> notexistColumns = new ArrayList<>();
        if (notexistColumn != null) {
            notexistColumns.add(notexistColumn);
        } else {
            notexistColumns = null;
        }

        JavaPairRDD result = scan(jsc, tableName, existColumns, notexistColumns)
                .mapToPair(row -> getRowResult(row, resultColumns))
                .filter(row -> row != null)
                .persist(StorageLevel.MEMORY_AND_DISK_SER_2())
                .repartition(conf.getInt(PARTITIONS, 48));

        return result;
    }
}
