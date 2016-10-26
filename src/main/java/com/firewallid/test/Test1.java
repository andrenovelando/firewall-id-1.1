/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.firewallid.test;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import org.apache.hadoop.mapred.lib.db.DBWritable;

/**
 *
 * @author andrenovelando@gmail.com
 */
public class Test1 implements DBWritable {

    public int id;
    public String c1;
    public String c2;

    public Test1(int id, String c1, String c2) {
        this.id = id;
        this.c1 = c1;
        this.c2 = c2;
    }

    @Override
    public void write(PreparedStatement ps) throws SQLException {
        ps.setInt(1, id);
        ps.setString(2, c1);
        ps.setString(3, c2);
    }

    @Override
    public void readFields(ResultSet rs) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

}
