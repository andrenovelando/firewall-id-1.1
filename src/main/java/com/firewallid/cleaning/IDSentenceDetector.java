/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.firewallid.cleaning;

import com.firewallid.base.Base;
import java.lang.reflect.InvocationTargetException;
import java.util.List;

/**
 *
 * @author andrenovelando@gmail.com
 */
public class IDSentenceDetector extends Base {

    Class isdClass;
    Object isdObj;

    public IDSentenceDetector() {
        super();
        try {
            isdClass = Class.forName("IndonesianSentenceDetector");
        } catch (ClassNotFoundException ex) {
            LOG.error(ex.getMessage());
        }
        try {
            isdObj = isdClass.newInstance();
        } catch (InstantiationException | IllegalAccessException ex) {
            LOG.error(ex.getMessage());
        }
    }

    public List<String> splitSentence(String text) {
        Class[] params = new Class[1];
        params[0] = String.class;

        try {
            return (List<String>) isdClass.getDeclaredMethod("splitSentence", params).invoke(isdObj, text);
        } catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException | NoSuchMethodException | SecurityException ex) {
            LOG.error(ex.getMessage());
            return null;
        }
    }
}
