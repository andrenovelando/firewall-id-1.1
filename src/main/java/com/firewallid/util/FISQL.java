/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.firewallid.util;

import com.google.common.base.Joiner;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;
import org.apache.commons.lang.StringUtils;

/**
 *
 * @author andrenovelando@gmail.com
 */
public class FISQL {

    public static String getFirstFieldInsertIfNotExist(Connection conn, String tableName, String field, Map<String, String> fields) throws SQLException {
        /* Query */
        String query = "SELECT " + field + " FROM " + tableName + " WHERE " + Joiner.on(" = ? AND ").join(fields.keySet()) + " = ?";

        /* Execute */
        PreparedStatement pst = conn.prepareStatement(query);
        int i = 1;
        for (String value : fields.values()) {
            pst.setString(i, value);
            i++;
        }
        ResultSet executeQuery = pst.executeQuery();
        if (executeQuery.next()) {
            return executeQuery.getString(field);
        }

        /* Row is not exists. Insert */
        query = "INSERT INTO " + tableName + " (" + Joiner.on(", ").join(fields.keySet()) + ") VALUES (" + StringUtils.repeat("?, ", fields.size() - 1) + "?)";
        pst = conn.prepareStatement(query);
        i = 1;
        for (String value : fields.values()) {
            pst.setString(i, value);
            i++;
        }
        if (pst.execute()) {
            return null;
        }
        return getFirstFieldInsertIfNotExist(conn, tableName, field, fields);
    }

    public static void updateRowInsertIfNotExist(Connection conn, String tableName, Map<String, String> updateConditions, Map<String, String> fields) throws SQLException {
        /* Query */
        String query = "SELECT " + Joiner.on(", ").join(updateConditions.keySet()) + " FROM " + tableName + " WHERE " + Joiner.on(" = ? AND ").join(updateConditions.keySet()) + " = ?";

        /* Execute */
        PreparedStatement pst = conn.prepareStatement(query);
        int i = 1;
        for (String value : updateConditions.values()) {
            pst.setString(i, value);
            i++;
        }
        ResultSet executeQuery = pst.executeQuery();
        if (executeQuery.next()) {
            /* Update */
            query = "UPDATE " + tableName + " SET " + Joiner.on(" = ?, ").join(fields.keySet()) + " = ? WHERE " + Joiner.on(" = ? AND ").join(updateConditions.keySet()) + " = ?";
            pst = conn.prepareStatement(query);
            i = 1;
            for (String value : fields.values()) {
                pst.setString(i, value);
                i++;
            }
            for (String value : updateConditions.values()) {
                pst.setString(i, value);
                i++;
            }
            pst.executeUpdate();
            return;
        }

        /* Row is not exists. Insert */
        query = "INSERT INTO " + tableName + " (" + Joiner.on(", ").join(fields.keySet()) + ", " + Joiner.on(", ").join(updateConditions.keySet()) + ") VALUES (" + StringUtils.repeat("?, ", fields.size() + updateConditions.size() - 1) + "?)";
        pst = conn.prepareStatement(query);
        i = 1;
        for (String value : fields.values()) {
            pst.setString(i, value);
            i++;
        }
        for (String value : updateConditions.values()) {
            pst.setString(i, value);
            i++;
        }
        pst.execute();
    }

}
