/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.firewallid.crawling;

import com.firewallid.base.Base;
import com.gargoylesoftware.htmlunit.BrowserVersion;
import facebook4j.Comment;
import facebook4j.Facebook;
import facebook4j.FacebookException;
import facebook4j.FacebookFactory;
import facebook4j.Group;
import facebook4j.PagableList;
import facebook4j.Page;
import facebook4j.Paging;
import facebook4j.Post;
import facebook4j.Reading;
import facebook4j.ResponseList;
import facebook4j.User;
import facebook4j.auth.AccessToken;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;
import org.jsoup.Jsoup;
import org.openqa.selenium.By;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.htmlunit.HtmlUnitDriver;
import scala.Tuple2;

/**
 *
 * @author andrenovelando@gmail.com
 */
public class FacebookApp extends Base {

    public static final String DATEFORMAT = "firewallindonesia.dateformat";

    public static final String APPS = "crawling.facebook.apps";
    public static final String LANGUAGE_TAG_ID = "crawling.facebook.languagetagid";
    public static final String PRIVACY_OPEN = "crawling.facebook.privacyopen";
    public static final String EMAIL_FIELD_ID = "crawling.facebook.html.login.emailid";
    public static final String PASSWORD_FIELD_ID = "crawling.facebook.html.login.passwordid";
    public static final String LOGIN_BUTTON_ID = "crawling.facebook.html.login.buttonid";
    public static final String EMAIL = "crawling.facebook.email";
    public static final String PASSWORD = "crawling.facebook.password";
    public static final String POST_CLASS = "crawling.facebook.html.post.class";

    public static final String FACEBOOK_URL = "https://www.facebook.com/";

    Facebook facebook;
    int appIdx;
    String[] apps;
    WebDriver webDriver;

    public FacebookApp() {
        conf.addResource("auth.xml");
        conf.addResource("facebook-default.xml");
        appIdx = 0;
        apps = conf.getStrings(APPS);
        facebook = createFacebook();
        webDriver = createWebDriver();
    }

    /* Create Facebook instance */
    private Facebook createFacebook() {
        String[] currentApp = conf.getStrings(String.format("%s.%s", APPS, apps[appIdx]));
        Facebook fb = new FacebookFactory().getInstance();
        fb.setOAuthAppId(currentApp[0], currentApp[1]);
        fb.setOAuthAccessToken(new AccessToken(currentApp[2]));
        return fb;
    }

    /* Create WebDriver instance */
    private WebDriver createWebDriver() {
        WebDriver webdriver = new HtmlUnitDriver(BrowserVersion.BEST_SUPPORTED, true);
        webdriver.get(FACEBOOK_URL);
        webdriver.findElement(By.id(conf.get(EMAIL_FIELD_ID))).sendKeys(conf.get(EMAIL));
        webdriver.findElement(By.id(conf.get(PASSWORD_FIELD_ID))).sendKeys(conf.get(PASSWORD));
        webdriver.findElement(By.id(conf.get(LOGIN_BUTTON_ID))).click();

        return webdriver;
    }

    /* GETTER */
    public Facebook getFacebook() {
        return facebook;
    }

    public WebDriver getWebDriver() {
        return webDriver;
    }

    /* Facebook's post */
    public Post post(String postId, String field) {
        try {
            return facebook.getPost(postId, new Reading().fields(field));
        } catch (FacebookException ex) {
            LOG.error(String.format("APP ID: %s. %s", apps[appIdx], ex.toString()));
            return post(postId, field);
        }
    }

    /* Facebook's comment */
    public Comment comment(String commentId) {
        try {
            return facebook.getComment(commentId);
        } catch (FacebookException ex) {
            LOG.error(String.format("APP ID: %s. %s", apps[appIdx], ex.toString()));
            return comment(commentId);
        }
    }

    /* Change Facebook instaces */
    public void changeFacebook() {
        if (appIdx == apps.length - 1) {
            appIdx = 0;
        } else {
            appIdx++;
        }
        facebook = createFacebook();
        LOG.info(String.format("Change Facebook App. Now use App ID %s", apps[appIdx]));
    }

    /* Search groups with language tag ID */
    public ResponseList<Group> searchGroupID(String keyword, int limit) {
        while (true) {
            try {
                return facebook.searchGroups(keyword, new Reading().limit(limit).locale(Locale.forLanguageTag(conf.get(LANGUAGE_TAG_ID))));
            } catch (FacebookException ex) {
                LOG.error(String.format("APP ID: %s. %s", apps[appIdx], ex.toString()));
                changeFacebook();
            }
        }
    }

    /* Search pages with language tag ID */
    public ResponseList<Page> searchPageID(String keyword, int limit) {
        while (true) {
            try {
                return facebook.searchPages(keyword, new Reading().limit(limit).locale(Locale.forLanguageTag(conf.get(LANGUAGE_TAG_ID))));
            } catch (FacebookException ex) {
                LOG.error(String.format("APP ID: %s. %s", apps[appIdx], ex.toString()));
                changeFacebook();
            }
        }
    }

    /* Search users with language tag ID */
    public ResponseList<User> searchUserID(String keyword, int limit) {
        while (true) {
            try {
                return facebook.searchUsers(keyword, new Reading().limit(limit).locale(Locale.forLanguageTag(conf.get(LANGUAGE_TAG_ID))));
            } catch (FacebookException ex) {
                LOG.error(String.format("APP ID: %s. %s", apps[appIdx], ex.toString()));
                changeFacebook();
            }
        }
    }

    /* Return true if group is open */
    public boolean isGroupOpen(Group group) {
        return String.valueOf(group.getPrivacy()).equals(conf.get(PRIVACY_OPEN));
    }

    /* Return feed from group/page */
    public ResponseList<Post> getFeeds(String id, int limit) {
        while (true) {
            try {
                return facebook.getFeed(id, new Reading().limit(limit).locale(Locale.forLanguageTag(conf.get(LANGUAGE_TAG_ID))));
            } catch (FacebookException ex) {
                LOG.error(String.format("APP ID: %s. %s", apps[appIdx], ex.toString()));
                changeFacebook();
            }
        }
    }

    public ResponseList<Post> getFeeds(String id, long sinceMillisTime) {
        SimpleDateFormat dateFormat = new SimpleDateFormat(conf.get(DATEFORMAT));

        while (true) {
            try {
                return facebook.getFeed(id, new Reading().since(dateFormat.format(new Date(sinceMillisTime))).until(dateFormat.format(new Date(System.currentTimeMillis()))));
            } catch (FacebookException ex) {
                LOG.error(String.format("APP ID: %s. %s", apps[appIdx], ex.toString()));
                changeFacebook();
            }
        }
    }

    /* Return user's feed */
    public List<String> getUserFeeds(String id) {
        WebDriver webDriver1 = createWebDriver();
        webDriver1.get(FACEBOOK_URL + id);

        List<String> feeds = Jsoup
                /* Get page source code */
                .parse(webDriver1.getPageSource())
                /* User's posts html tag */
                .getElementsByClass(conf.get(POST_CLASS))
                .parallelStream().map(postHtmlTag -> postHtmlTag.text())
                .filter(post -> !post.isEmpty())
                .collect(Collectors.toList());

        LOG.debug(String.format("User: %s. Total feed: %s", id, feeds.size()));

        return feeds;
    }

    /* Return list of comment */
    public List<Comment> getComments(Comment comment) {
        PagableList<Comment> commentsPage = post(comment.getId(), "comments").getComments();
        Paging<Comment> paging = commentsPage.getPaging();
        List<Comment> comments = new ArrayList<>();
        while (paging != null && commentsPage != null) {
            commentsPage.parallelStream().forEach(commentt -> comments.add(commentt));
            paging = commentsPage.getPaging();
            while (true) {
                try {
                    commentsPage = facebook.fetchNext(paging);
                    break;
                } catch (FacebookException ex) {
                    LOG.error(String.format("APP ID: %s. %s", apps[appIdx], ex.toString()));
                    changeFacebook();
                }
            }
        }

        return comments;
    }

    public List<Tuple2<Comment, Integer>> getComments(Comment commentParent, int depth) {
        List<Tuple2<Comment, Integer>> comments = getComments(commentParent)
                .parallelStream()
                .map(comment -> new Tuple2<>(comment, depth))
                .collect(Collectors.toList());

        List<Tuple2<Comment, Integer>> commentsChild = comments
                .parallelStream()
                .flatMap(comment -> getComments(comment._1(), depth + 1).parallelStream())
                .collect(Collectors.toList());

        comments.addAll(commentsChild);

        return comments;
    }

    public List<Tuple2<Comment, Integer>> getComments(String postId) {
        return getComments(comment(postId), 1);
    }
;
}
