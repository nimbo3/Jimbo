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
        assertTrue(crawlProtected.isValidUri("http://stackoverflow.com/"));
        assertTrue(crawlProtected.isValidUri("https://stackoverflow.com/"));
        assertTrue(crawlProtected.isValidUri("http://stackoverflow.com"));
        assertTrue(crawlProtected.isValidUri("https://stackoverflow.co.io/"));
        assertTrue(crawlProtected.isValidUri("https://discuss.stackoverflow.co.io"));
        assertTrue(crawlProtected.isValidUri("http://discuss.stackoverflow.co.io"));
        assertTrue(crawlProtected.isValidUri("www.w3school.com/javaCourse.asp"));
        assertTrue(crawlProtected.isValidUri("www.w3school.com/javaCourse.htm"));
        assertTrue(crawlProtected.isValidUri("www.w3school.com/javaCourse.html"));
        assertFalse(crawlProtected.isValidUri("abc.jpg"));
        assertFalse(crawlProtected.isValidUri(""));
        assertFalse(crawlProtected.isValidUri("/"));
        assertFalse(crawlProtected.isValidUri("www."));
        assertFalse(crawlProtected.isValidUri("felan.mkv"));
        assertFalse(crawlProtected.isValidUri("www.felan.mkv"));
        assertFalse(crawlProtected.isValidUri("https://www.felan.mkv"));
        assertFalse(crawlProtected.isValidUri("https://felan.mkv"));
        assertFalse(crawlProtected.isValidUri("www.w3school.com/javaCourse.aspx"));
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