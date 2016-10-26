/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.firewallid.crawling;

import com.firewallid.base.Base;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import scala.Tuple3;
import twitter4j.Query;
import twitter4j.QueryResult;
import twitter4j.Status;
import twitter4j.Twitter;
import twitter4j.TwitterException;
import twitter4j.TwitterFactory;
import twitter4j.User;
import twitter4j.auth.AccessToken;

/**
 *
 * @author andrenovelando@gmail.com
 */
public class TwitterApp extends Base {

    public static final String APPS = "crawling.twitter.apps";

    Twitter twitter;
    int appIdx;
    String[] apps;

    public TwitterApp() {
        conf.addResource("auth.xml");
        conf.addResource("twitter-default.xml");
        appIdx = 0;
        apps = conf.getStrings(APPS);
        twitter = createTwitter();
    }

    /* Create Twitter instance */
    private Twitter createTwitter() {
        String[] currentApp = conf.getStrings(String.format("%s.%s", APPS, apps[appIdx])); //0:CONSUMER_KEY 1:CONSUMER_KEY_SECRET 2:ACCESS_TOKEN 3:ACCESS_TOKEN_SECRET

        Twitter tw = new TwitterFactory().getInstance();
        tw.setOAuthConsumer(currentApp[0], currentApp[1]);
        AccessToken accessToken = new AccessToken(currentApp[2], currentApp[3]);
        tw.setOAuthAccessToken(accessToken);

        return tw;
    }

    /* GETTER */
    public Twitter getTwitter() {
        return twitter;
    }

    /* Change Twitter instaces */
    public void changeTwitter() {
        if (appIdx == apps.length - 1) {
            appIdx = 0;
        } else {
            appIdx++;
        }
        twitter = createTwitter();
        LOG.info(String.format("Change Twitter App. Now use App ID: %s", apps[appIdx]));
    }

    /* Get user */
    public User user(String userId) {
        try {
            return twitter.showUser(Long.parseLong(userId));
        } catch (TwitterException ex) {
            LOG.error(String.format("user. APP ID: %s. %s", apps[appIdx], ex.toString()));
            changeTwitter();
            return user(userId);
        }
    }

    /* Get status */
    public Status status(String tweetId) {
        try {
            return twitter.showStatus(Long.parseLong(tweetId));
        } catch (TwitterException ex) {
            LOG.error(String.format("tweet. APP ID: %s. %s", apps[appIdx], ex.toString()));
            changeTwitter();
            return status(tweetId);
        }
    }

    /* Return true if user is protected */
    public boolean isProtected(String userId) {
        return user(userId).isProtected();
    }

    public QueryResult search(Query query) {
        try {
            return twitter.search(query);
        } catch (TwitterException ex) {
            LOG.error(String.format("search. APP ID: %s. %s", apps[appIdx], ex.toString()));
            changeTwitter();
            return search(query);
        }
    }

    /* Return all status reply to tweetId */
    public List<Status> replyTo(Status status) {
        String userScreenName = status.getUser().getScreenName();
        String date = new SimpleDateFormat("YYYY-MM-DD").format(status.getCreatedAt());
        Query query = new Query("to:" + userScreenName + " since:" + date);
        List<Status> repliesAll = new ArrayList<>();

        while (query != null) {
            QueryResult result = search(query);

            List<Status> replies = result
                    .getTweets()
                    .parallelStream()
                    /* Only reply to status's id*/
                    .filter(tweet -> tweet.getInReplyToStatusId() == status.getId())
                    .collect(Collectors.toList());

            repliesAll.addAll(replies);
            query = result.nextQuery();
        }

        return repliesAll;
    }

    /* Return all status reply to tweetId, include their child. List of (statusChild, statusParent) */
    public List<Tuple3<Status, Status, Integer>> replyToTree(Status statusParent, int depth) {
        List<Tuple3<Status, Status, Integer>> replies = replyTo(statusParent)
                .parallelStream()
                .map(statusChild -> new Tuple3<>(statusChild, statusParent, depth))
                .collect(Collectors.toList());

        List<Tuple3<Status, Status, Integer>> repliesChild = replies
                .parallelStream()
                .flatMap(status -> replyToTree(status._1(), depth + 1).parallelStream())
                .collect(Collectors.toList());

        replies.addAll(repliesChild);

        return replies;
    }

    /* Return all status contains keyword */
    public List<Status> searchTweet(String keyword, int limit) {
        List<Status> result = new ArrayList<>();
        Query query = new Query(keyword);
        while (query != null && result.size() <= limit) {
            QueryResult search = search(query);
            result.addAll(search.getTweets());
            query = search.nextQuery();
        }
        return result;
    }
}
