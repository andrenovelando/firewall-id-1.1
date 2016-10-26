/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.firewallid.util;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.logging.Level;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.jsoup.Jsoup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author andrenovelando@gmail.com
 */
public class FIConnection {

    public static final Logger LOG = LoggerFactory.getLogger(FIConnection.class);

    public static String getText(String url, int timeoutSeconds) {
        String text = "";
        long startTime = System.currentTimeMillis();
        while (true) {
            try {
                text = Jsoup.connect(url).userAgent("Mozilla").get().text();
                LOG.info(String.format("Downloaded. Url: %s", url));
                break;
            } catch (IOException ex) {
                LOG.error(String.format("Url: %s. %s", url, ex.getMessage()));

                long runningTime = System.currentTimeMillis() - startTime;
                if (runningTime / 1000 >= timeoutSeconds) {
                    LOG.error(String.format("Url: %s. Timeout reached", url));
                    break;
                }
            }
        }

        return text;
    }

    public static void sendJson(String host, String json) throws UnsupportedEncodingException, IOException {
        DefaultHttpClient httpClient = new DefaultHttpClient();
        HttpPost postRequest = new HttpPost(host);

        StringEntity input = new StringEntity(json);
        input.setContentType("application/json");
        postRequest.setEntity(input);

        HttpResponse response = httpClient.execute(postRequest);
    }
}
