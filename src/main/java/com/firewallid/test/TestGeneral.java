/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.firewallid.test;

import com.codepoetics.protonpack.Indexed;
import com.codepoetics.protonpack.StreamUtils;
import com.cybozu.labs.langdetect.LangDetectException;
import com.firewallid.cleaning.Cleaning;
import com.firewallid.cleaning.IDPOSTagger;
import com.firewallid.cleaning.IDSentenceDetector;
import com.firewallid.crawling.FacebookApp;
import com.firewallid.crawling.FacebookIDSearch;
import com.firewalld.sentimentanalysis.IDSentiWordNet;
import com.firewalld.sentimentanalysis.SentimentAnalysis;
import com.firewallid.util.FIConfiguration;
import com.firewallid.util.FIUtils;
import com.google.common.base.Joiner;
import com.kenai.constantine.platform.darwin.SocketOption;
import facebook4j.Comment;
import facebook4j.FacebookException;
import facebook4j.Post;
import facebook4j.ResponseList;
import facebook4j.User;
import java.io.DataOutputStream;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.openqa.grid.internal.utils.configuration.StandaloneConfiguration;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.htmlunit.HtmlUnitDriver;
import org.openqa.selenium.remote.server.SeleniumServer;
import scala.Tuple2;
import scala.Tuple3;

/**
 *
 * @author andrenovelando@gmail.com
 */
public class TestGeneral {

    String a;
    static String s;

    public TestGeneral() {
        a = "a";
        set();
    }

    private void set() {
        s = "a";
    }

    public void print() {
        System.out.println(s);
    }

    public static void main(String[] args) throws ClassNotFoundException, SQLException, LangDetectException, InterruptedException, InstantiationException, IllegalAccessException, NoSuchMethodException, IllegalArgumentException, InvocationTargetException, IOException, FacebookException {
        URL url = new URL("http://localhost:3030/widgets/welcome");
        HttpURLConnection con = (HttpURLConnection) url.openConnection();

        //  CURLOPT_POST
        con.setRequestMethod("POST");

        // CURLOPT_FOLLOWLOCATION
//        con.setInstanceFollowRedirects(true);
        String postData = "my_data_for_posting";
        con.setRequestProperty("auth_token", "YOUR_AUTH_TOKEN");
        con.setRequestProperty("text", "Hey, Look what I can do!");

        con.setDoOutput(true);
        con.setDoInput(true);
        DataOutputStream output = new DataOutputStream(con.getOutputStream());
        output.writeBytes(postData);
        output.close();
        con.getResponseCode();
    }
}
