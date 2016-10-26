/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.firewallid.cleaning;

import com.firewallid.base.Base;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.List;

/**
 *
 * @author andrenovelando@gmail.com
 */
public class IDPOSTagger extends Base {

    Object ipt;
    Method doPOSTag;

    public IDPOSTagger() throws ClassNotFoundException, InstantiationException, IllegalAccessException, NoSuchMethodException {
        super();
        ipt = Class.forName("IndonesianPOSTagger").newInstance();
        doPOSTag = doPOSTag();
    }

    private Method doPOSTag() throws ClassNotFoundException, NoSuchMethodException {
        Class[] params = new Class[1];
        params[0] = String.class;
        return Class.forName("IndonesianPOSTagger").getDeclaredMethod("doPOSTag", params);
    }

    public List<String[]> doPOSTag(String sentence) throws IllegalAccessException, IllegalArgumentException, InvocationTargetException {
        return (List<String[]>) doPOSTag.invoke(ipt, sentence);
    }
}
