package com.firewallid.cleaning;

import com.firewallid.base.Base;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.StringTokenizer;

/**
 *
 * @author andrenovelando@gmail.com
 */
public class IDRemoval extends Base {

    String alamatStopList = "data/stoplist.txt";
    String alamatExclusion = "data/exclusion.txt";
    ArrayList<String> stopList = new ArrayList();
    ArrayList<String> exclusion = new ArrayList();
    Class isClass;
    Object isObject;
    Method stem;
    Class istClass;
    Object istObject;
    Method tokenizeSentenceWithCompositeWords;

    public IDRemoval() {
        super();
        try {
            istClass = Class.forName("IndonesianSentenceTokenizer");
        } catch (ClassNotFoundException ex) {
            LOG.error(ex.getMessage());
        }
        try {
            istObject = istClass.newInstance();
        } catch (InstantiationException | IllegalAccessException ex) {
            LOG.error(ex.getMessage());
        }
        tokenizeSentenceWithCompositeWords = tokenizeSentenceWithCompositeWords();
        try {
            isClass = Class.forName("IndonesianStemmer");
        } catch (ClassNotFoundException ex) {
            LOG.error(ex.getMessage());
        }
        try {
            isObject = isClass.newInstance();
        } catch (InstantiationException | IllegalAccessException ex) {
            LOG.error(ex.getMessage());
        }
        stem = stem();
        setStopList();
        setExclusion();
    }

    /* SETTER */
    private Method tokenizeSentenceWithCompositeWords() {
        Class[] params = new Class[1];
        params[0] = String.class;

        try {
            return istClass.getDeclaredMethod("tokenizeSentenceWithCompositeWords", params);
        } catch (NoSuchMethodException | SecurityException ex) {
            LOG.error(ex.getMessage());
            return null;
        }
    }

    private Method stem() {
        Class[] params = new Class[1];
        params[0] = String.class;

        try {
            return isClass.getDeclaredMethod("stem", params);
        } catch (NoSuchMethodException | SecurityException ex) {
            LOG.error(ex.getMessage());
            return null;
        }
    }

    /*Meload daftar kata pengecualian*/
    private void setExclusion() {
        try {
            BufferedReader readStopList;
            readStopList = new BufferedReader(new FileReader(alamatExclusion));
            String Line;
            while ((Line = readStopList.readLine()) != null) {
                StringTokenizer String = new StringTokenizer(Line, ",");
                while (String.hasMoreTokens()) {
                    exclusion.add(String.nextToken());
                }
            }
            readStopList.close();
        } catch (FileNotFoundException ex) {
            LOG.error("Error baca stop list!");
        } catch (IOException ex) {
            LOG.error("Error baca line!");
        }
    }

    /*Meload daftar stoplist*/
    private void setStopList() {
        try {
            BufferedReader readStopList;
            readStopList = new BufferedReader(new FileReader(alamatStopList));
            String Line;
            while ((Line = readStopList.readLine()) != null) {
                StringTokenizer String = new StringTokenizer(Line, ",");
                while (String.hasMoreTokens()) {
                    stopList.add(String.nextToken());
                }
            }
            readStopList.close();
        } catch (FileNotFoundException ex) {
            LOG.error("Error baca stop list!");
        } catch (IOException ex) {
            LOG.error("Error baca line!");
        }
    }

    /*remove tanda baca*/
    public String removePunctuation(String cek) {
        String temp = "";
        for (int x = 0; x < cek.length(); x++) {
            if (("" + cek.charAt(x)).matches("[a-zA-Z0-9-]")) {
                if (temp.equals("")) {
                    temp = cek.charAt(x) + "";
                } else {
                    temp = temp + cek.charAt(x);
                }
            } else if (((x + 1) < cek.length()) && (("" + cek.charAt(x + 1)).matches("[a-zA-Z0-9]"))) {
                temp = temp + " ";
            }
        }
        return temp;
    }

    /*remove stopword*/
    public String stopwordRemoval(String kata) {
        String temp = kata;

        boolean found = stopList
                .parallelStream()
                .map((String stop) -> {
                    boolean dapat = false;

                    if (stop.equals(temp)) {
                        dapat = true;
                    }
                    return dapat;
                }).anyMatch(s -> s == true);

        if ((found) || (kata.matches("[ ?[0-9] ?]+"))) {
            kata = "";
        } else //perbaiki pengecekan angka
        if (kata.matches("\\d")) {
            kata = "";
        }
        return kata;
    }

    /*stemming*/
    public String stem(String kata) {
        try {
            return (String) stem.invoke(isObject, kata);
        } catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException ex) {
            LOG.error(ex.getMessage());
            return null;
        }
    }

    public String stemming(String kata) {
        String temp = kata;
        boolean found = exclusion
                .parallelStream()
                .map((String pengecualian) -> {
                    boolean dapat = false;

                    if (pengecualian.equals(temp)) {
                        dapat = true;
                    }
                    return dapat;
                }).anyMatch(s -> s == true);
        if (!found) {
            kata = stem(kata);
        }
        return kata;
    }

    /*tokenisasi kalimat dengan INANLP*/
    public ArrayList<String> tokenisasi(String kalimat) {
        try {
            return (ArrayList<String>) tokenizeSentenceWithCompositeWords.invoke(istObject, kalimat);
        } catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException ex) {
            LOG.error(ex.getMessage());
            return null;
        }
    }
}
