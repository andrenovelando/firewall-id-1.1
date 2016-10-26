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
public class IDFormalization extends Base {

    String alamatAkronim = "data/akronim.txt";
    ArrayList<String> daftarAkronim = new ArrayList();
    String[] listTanda = {"...", ",", ";", ":", ".", "--", "?", "(", ")", "[", "]", "{", "}", "<", ">", "'", "\"", "/"};
    Class isfClass;
    Object isfObj;
    Method formalizeWord;
    Class istClass;
    Object istObject;
    Method tokenizeSentenceWithCompositeWords;

    public IDFormalization() {
        super();
        try {
            isfClass = Class.forName("IndonesianSentenceFormalization");
        } catch (ClassNotFoundException ex) {
            LOG.error(ex.getMessage());
        }
        try {
            isfObj = isfClass.newInstance();
        } catch (InstantiationException | IllegalAccessException ex) {
            LOG.error(ex.getMessage());
        }
        formalizeWord = formalizeWord();
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
        setDaftarAkronim();
    }

    /*Meload data Akronim yang diperlukan*/
    private void setDaftarAkronim() {
        try {
            BufferedReader readDaftar;
            readDaftar = new BufferedReader(new FileReader(alamatAkronim));
            String line;
            while ((line = readDaftar.readLine()) != null) {
                StringTokenizer tokenString = new StringTokenizer(line, ",");
                while (tokenString.hasMoreTokens()) {
                    daftarAkronim.add(tokenString.nextToken());
                }
            }
            readDaftar.close();
        } catch (FileNotFoundException ex) {
            LOG.error("File akronim tidak ditemukan");
        } catch (IOException ex) {
            LOG.error("error baca file akronim");
        }
    }

    /*Mereplace akronim dengan bahasa formal*/
    public String akronimLookUp(String word) {
        if (word.matches("[a-zA-Z]+[.]+([a-zA-Z]?+[.]?)*")) {
            for (String akronim : daftarAkronim) {
                String[] tempAkronim = akronim.split(":");
                if (word.equals(tempAkronim[0].toLowerCase())) {
                    word = word.replace(tempAkronim[0].toLowerCase(), tempAkronim[1].toLowerCase());
                } else {
                    word = word.replace(".", "");
                }
            }
        }
        return word;
    }

    /*untuk mengesplit kata gandeng, seperti pada hashtag*/
    public String splitAttach(String word) {
        String tempCh = "";
        for (int i = 0; i < word.length(); i++) {
            if (("" + word.charAt(i)).matches("[A-Z]")) {
                if (tempCh.equals("")) {
                    tempCh = word.charAt(i) + "";
                } else if ((((i - 1) >= 0) && (("" + word.charAt(i - 1)).matches("[a-z0-9]"))) || (((i + 1) < word.length()) && (("" + word.charAt(i + 1)).matches("[a-z0-9]")))) {
                    tempCh = tempCh + " " + word.charAt(i);
                } else {
                    tempCh = tempCh + word.charAt(i);
                }
            } else {
                tempCh = tempCh + word.charAt(i);
            }
        }
        return tempCh;
    }

    /*Untuk memisahkan angka yang gandeng dengan kata*/
    public String normalisasi(String kata) {
        kata = kata.replace("[ ][0-9]", "");
        for (String tanda : listTanda) {
            kata = kata.replace(tanda, " " + tanda + " ");
        }
        return kata;
    }

    /*Remove simbol pada kata*/
    public String removeSimbol(String kata) {
        String hasil = "";
        for (int i = 0; i < kata.length(); i++) {
            if ((kata.charAt(i) + "").matches("[a-z0-9@$!?.,; :-]")) {
                if (hasil.equals("")) {
                    hasil = kata.charAt(i) + "";
                } else {
                    hasil = hasil + kata.charAt(i);
                }
            }
        }
        return hasil;
    }

    /*Remove URL dari teks*/
    public String removeURL(String kata) {
        String hasil = "";
        if ((!kata.matches("^(https?|ftp|file)://[-a-zA-Z0-9+&@#/%?=~_|!:,.;]*[-a-zA-Z0-9+&@#/%=~_|]")) && (!kata.matches("w.[-a-zA-Z0-9+&@#/%?=~_|!:,.;]*[-a-zA-Z0-9+&@#/%=~_|]"))) {
            if (!kata.matches("^[a-zA-Z0-9_.]+@[a-zA-Z0-9]+.[a-zA-Z0-9]+$")) {
                hasil = kata;
            }
        }
        return hasil;
    }

    /*Standarisasi kata dengan INANLP*/
    private Method formalizeWord() {
        Class[] params = new Class[1];
        params[0] = String.class;

        try {
            return isfClass.getDeclaredMethod("formalizeWord", params);
        } catch (NoSuchMethodException | SecurityException ex) {
            LOG.error(ex.getMessage());
            return null;
        }
    }

    public String standarisasi(String kata) {
        try {
            return (String) formalizeWord.invoke(isfObj, kata);
        } catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException ex) {
            LOG.error(ex.getMessage());
            return null;
        }
    }

    /*Tokenisasi kalimat dengan INANLP*/
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

    public ArrayList<String> tokenisasi(String kalimat) {
        try {
            return (ArrayList<String>) tokenizeSentenceWithCompositeWords.invoke(istObject, kalimat);
        } catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException ex) {
            LOG.error(ex.getMessage());
            return null;
        }
    }
}
