/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.firewallid.io;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author andrenovelando@gmail.com
 */
public class HBaseTableUtils {

    public static final Logger LOG = LoggerFactory.getLogger(HBaseTableUtils.class);

    public static String getFamily(String column) {
        return column.split(":")[0];
    }

    public static boolean istableExists(String tableName) throws IOException {
        if (new HBaseAdmin(HBaseConfiguration.create()).tableExists(tableName)) {
            LOG.info("Connected to Table " + tableName);
            return true;
        } else {
            LOG.warn("Table " + tableName + " is not exist. ");
            return false;
        }
    }

    /* Create tableName if not exist */
    public static void createTable(String tableName, List<String> familys) throws IOException {
        if (!istableExists(tableName)) {
            HTableDescriptor tableDesc = new HTableDescriptor(tableName);
            familys.parallelStream().forEach((String family) -> tableDesc.addFamily(new HColumnDescriptor(family)));
            new HBaseAdmin(HBaseConfiguration.create()).createTable(tableDesc);
            LOG.info(String.format("Table %s is created", tableName));
        }
    }
    
    /* True if column family exist */
    public static boolean isFamilyExists(HTable hTable, String family) throws IOException {
        return hTable.getTableDescriptor().hasFamily(Bytes.toBytes(family));
    }

    public static boolean isFamilyExists(String tableName, String family) throws IOException {
        HTable hTable = new HTable(HBaseConfiguration.create(), tableName);
        if (isFamilyExists(hTable, family)) {
            LOG.info("Table " + tableName + " has column family " + family);
            return true;
        } else {
            LOG.warn("Table " + tableName + " has not column family " + family);
            return false;
        }
    }

    public static boolean isFamilyExists(String tableName, List<String> familys) throws IOException {
        for (String family : new HashSet<>(familys)) {
            if (!isFamilyExists(tableName, family)) {
                return false;
            }
        }
        return true;
    }
}
