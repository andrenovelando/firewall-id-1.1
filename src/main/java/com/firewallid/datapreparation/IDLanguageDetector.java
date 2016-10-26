/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.firewallid.datapreparation;

import com.cybozu.labs.langdetect.Detector;
import com.cybozu.labs.langdetect.DetectorFactory;
import com.cybozu.labs.langdetect.LangDetectException;
import com.firewallid.base.Base;

/**
 *
 * @author andrenovelando@gmail.com
 */
public class IDLanguageDetector extends Base {

    public static final String PROFILES = "datapreparation.profiles.dir";

    public IDLanguageDetector() {
        super();
        try {
            DetectorFactory.loadProfile(conf.get(PROFILES));
        } catch (LangDetectException ex) {
            LOG.error(ex.getMessage());
        }
    }

    /* Return true if text in indonesian language */
    public boolean detect(String text) throws LangDetectException {
        Detector detector = DetectorFactory.create();

        try {
            detector.append(text);
            String detect = detector.detect();
            LOG.debug(String.format("TEXT: %s \nLANGUAGE: %s", text, detect));
            return "id".equals(detector.detect());
        } catch (LangDetectException ex) {
            LOG.error(String.format("%s. TEXT: \" %s \"", ex.toString(), text));
            return false;
        }
    }
}
