package ir.jimbo.train.data;

import ir.jimbo.commons.exceptions.NoDomainFoundException;
import ir.jimbo.train.data.service.CacheService;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jsoup.Connection;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;

import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * class to crawl on special conditions.
 * There are 3 main condition that will be checked. anchor contain special keyWord, metas contain special keyWords,
 * and text content contains special keyWords at least n times where n must be given.
 * In case of any one of above conditions is true we assume that this url is passed.
 */
@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
public class CrawlProtected implements Runnable {

    private final Logger logger = LogManager.getLogger(this.getClass());
    /**
     * Regex pattern to extract domain from URL. Please refer to RFC 3986 - Appendix B for more information
     */
    private final Pattern domainPattern = Pattern.compile("^(([^:/?#]+):)?(//([^/?#]*))?([^?#]*)(\\?([^#]*))?(#(.*))?");
    private String seedUrl;
    private String anchor;
    /**
     * depth of crawl. number of layer that crawler goes in in the url and fetch page.
     * so if depth of crawl is zero only current url will be checked and crawler wont go to current page links.
     */
    private int crawlDepth;
    private int politenessTime;
    private boolean stayInDomain;
    /**
     * Words that site anchor must contain. it must contain all of them
     */
    private Set<String> anchorKeyWords;
    /**
     * keyWords in siteContent map to minimum number of repeats in it.
     * if a site hold one maps conditions in the set we assume that this site holds contentKeyWords condition.
     */
    private Set<Map<String, Integer>> contentKeyWords;
    /**
     * word that meta tags must contain. there are multiple tags that may lead to site content topic, like
     * description meta tag and topic meta tag and sub topic meta tag and keyWords meta tag etc.
     * that's why we search on all of meta tags. containing one keyWord is enough.
     */
    private Set<String> metaContain;

    private CacheService cacheService;

    @Override
    public void run() {
        try {
            logger.info("before: {}", seedUrl);
            seedUrl = new URL(seedUrl).toURI().normalize().toString();
            logger.info("after: {}", seedUrl);
            boolean isUrlYetPassed = false;
            if (anchorKeyWords != null && !anchorKeyWords.isEmpty() && checkAnchorKeyWord()) {
                addUrl();
                isUrlYetPassed = true;
            }
            Document pageDocument = null;
            Runtime.getRuntime().addShutdownHook(new Thread(cacheService::close));
            if (!isUrlYetPassed) {
                waitForPoliteness(cacheService);
                pageDocument = fetchUrl();
                if (checkContent(pageDocument)) {
                    addUrl();
                }
            }
            if (crawlDepth > 0) {
                if (pageDocument == null) {
                    waitForPoliteness(cacheService);
                    pageDocument = fetchUrl();
                }
                createNewUrlSeeds(pageDocument);
            }
        } catch (IOException | URISyntaxException e) {
            logger.error("exception in creating cache service...", e);
        }
    }

    public void addToThreadPool() {
        while (true) {
            int threadPoolQueueSize = App.executor.getActiveCount();
            if (threadPoolQueueSize <= 10) {
                App.executor.submit(this);
                break;
            }
            logger.info("Try again");
            try {
                Thread.sleep(5000);
            } catch (Exception e) {
                logger.error(e);
            }
        }
    }

    public boolean checkAnchorKeyWord() {
        if (anchorKeyWords == null || anchorKeyWords.isEmpty()) {
            return true;
        }
        int count = 0;
        for (String anchorKeyWord : anchorKeyWords) {
            if (anchor.toLowerCase().contains(anchorKeyWord))
                count++;
        }
        return count == anchorKeyWords.size();
    }

    private void createNewUrlSeeds(Document pageDocument) {
        for (Element a : pageDocument.getElementsByTag("a")) {
            String newAnchor = a.text();
            String url = a.attr("abs:href");
            if (!isValidUri(url))
                return;
            if (!stayInDomain || url.startsWith(seedUrl)) {
                new CrawlProtected(url, newAnchor, crawlDepth - 1, politenessTime,
                        stayInDomain, anchorKeyWords, contentKeyWords, metaContain, cacheService).addToThreadPool();
            }
        }
    }

    private void addUrl() {
        boolean flag;
        do {
            try {
                flag = App.passedUrls.offer(seedUrl, 100, TimeUnit.MILLISECONDS);
                Thread.sleep(1000);
            } catch (Exception e) {
                logger.error("exception while offering url {} in the queue", seedUrl, e);
                flag = true;
            }
        } while (!flag);
        logger.info("url {} passed and added to queue for saving", seedUrl);
    }

    public String getDomain(String url) {
        final Matcher matcher = domainPattern.matcher(url);
        String result = null;
        if (matcher.matches())
            result = matcher.group(4);
        if (result == null) {
            throw new NoDomainFoundException();
        }
        if (result.startsWith("www.")) {
            result = result.substring(4);
        }
        if (result.isEmpty()) {
            throw new NoDomainFoundException();
        }
        return result;
    }

    /**
     * @return True if uri end with ".html" or ".htm" or ".asp" or ".php" or the uri do not have any extension.
     */
    public boolean isValidUri(String link) {
        try {
            while (link.endsWith("/")) {
                link = link.substring(0, link.length() - 1);
            }
            if (link.trim().isEmpty())
                return false;
            if (link.endsWith(".html") || link.endsWith(".htm") || link.endsWith(".php") || link.endsWith(".asp")) {
                return true;
            }
            if (!link.substring(link.lastIndexOf('/') + 1).contains(".")
                    || link.lastIndexOf('/') == link.indexOf("//") + 1) {
                return true;
            }
        } catch (IndexOutOfBoundsException e) {
            logger.info("invalid uri : {}", link);
            return false;
        }
        return false;
    }

    public Document fetchUrl() {
        logger.info("start parsing...");
        Document document;
        try {
            Connection connect = Jsoup.connect(seedUrl);
            connect.timeout(10000);
            document = connect.get();
        } catch (Exception e) { //
            logger.error("exception in connection to url. empty page instance will return", e);
            return null;
        }
        return document;
    }

    private void waitForPoliteness(CacheService cacheService) {
        logger.info("checking for politeness and waiting until it become polite");
        String domain = getDomain(seedUrl);
        while (cacheService.isDomainExist(domain)) {
            try {
                logger.info("sleeping for politeness of {} url", seedUrl);
                Thread.sleep(1000);
            } catch (Exception e) {
                logger.error("interrupted while sleeping for politeness", e);
            }
        }
    }

    /**
     * Check site content and metas to check whether this site meet the given conditions or not
     *
     * @return true if the content and metas contains given conditions
     */
    public boolean checkContent(Document pageDocument) {
        if (checkMetasKeyWords(pageDocument)) {
            if (checkContentKeyWords(pageDocument)) {
                return true;
            } else {
                logger.info("page with link {}, passed due to containing minimum" +
                        " number of keyWords in content", seedUrl);
            }
        } else {
            logger.info("page with link {}, passed due to not containing meta keyWords", seedUrl);
        }
        return false;
    }

    public boolean checkContentKeyWords(Document pageDocument) {
        if (contentKeyWords == null || contentKeyWords.isEmpty())
            return true;
        String documentText = pageDocument.text().toLowerCase();
        for (Map<String, Integer> map : contentKeyWords) {
            for (Map.Entry<String, Integer> entry : map.entrySet()) {
                if (documentText.length() - documentText.replaceAll(entry.getKey(), "").length() >= entry.getValue()) {
                    return true;
                }
            }
        }
        return false;
    }

    public boolean checkMetasKeyWords(Document pageDocument) {
        if (metaContain == null || metaContain.isEmpty())
            return true;
        for (Element meta : pageDocument.getElementsByTag("meta")) {
            String text = meta.toString().toLowerCase();
            for (String s : metaContain) {
                if (text.contains(s))
                    return true;
            }
        }
        return false;
    }
}
