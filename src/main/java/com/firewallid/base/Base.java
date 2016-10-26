/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.firewallid.base;

import com.firewallid.datapreparation.IDLanguageDetector;
import com.firewallid.util.FIConfiguration;
import java.io.Serializable;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author andrenovelando@gmail.com
 */
public class Base implements Serializable {

    public Logger LOG;
    public static Configuration conf = FIConfiguration.create();

    public Base() {
        LOG = LoggerFactory.getLogger(getClass());
    }

}
