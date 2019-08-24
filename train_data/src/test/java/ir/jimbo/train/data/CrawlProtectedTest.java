package ir.jimbo.train.data;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

public class CrawlProtectedTest {

    private CrawlProtected crawlProtected;

    @Before
    public void init() {
        crawlProtected = new CrawlProtected();
    }

    @Test
    public void checkAnchorKeyWord() {
    }

    @Test
    public void getDomain() {
        assertEquals(crawlProtected.getDomain("http://stackoverflow.com/"), "stackoverflow.com");
        assertEquals(crawlProtected.getDomain("https://stackoverflow.com/"), "stackoverflow.com");
        assertEquals(crawlProtected.getDomain("http://stackoverflow.com"), "stackoverflow.com");
        assertEquals(crawlProtected.getDomain("https://stackoverflow.com/"), "stackoverflow.com");
        assertEquals(crawlProtected.getDomain("https://stackoverflow.co.io/"), "stackoverflow.co.io");
        assertEquals(crawlProtected.getDomain("https://disscus.stackoverflow.co.io"), "disscus.stackoverflow.co.io");
        assertEquals(crawlProtected.getDomain("https://www.disscus.stackoverflow.co.io"), "disscus.stackoverflow.co.io");
        String str1 = crawlProtected.getDomain("http://www.discuss.stackoverflow.co.io");
        String str2 = crawlProtected.getDomain("http://discuss.stackoverflow.co.io");
        assertEquals(str1, str2);
    }

    @Test
    public void isValidUri() {
    }

    @Test
    public void fetchUrl() {
    }

    @Test
    public void checkContent() {
    }

    @Test
    public void checkContentKeyWords() {
    }

    @Test
    public void checkMetasKeyWords() {
    }
}