package ir.jimbo.train.data;

import com.sun.net.httpserver.HttpServer;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.*;

public class CrawlProtectedTest {

    private static CrawlProtected crawlProtected;
    private HttpServer server1;
    private HttpServer server2;

    @BeforeClass
    public static void getClassInstance() {
        crawlProtected = new CrawlProtected();
    }

    @Before
    public void initServer1() throws IOException {
        String data = "<!DOCTYPE html><html lang=\"en\"><head><meta charset=\"UTF-8\"><title>Test page Title</title>" +
                " <meta name=\"title\" content=\"Test football meta tag\"/> <meta name=\"description\" content=\"sport" +
                " description tag\"/> <meta name=\"keywords\" content=\"test, java, junit\"></head><body><h1>Header1</h1>" +
                "<h2>Header2</h2><h3>Header3</h3><h4>Header4</h4><h5>Header5</h5><h6>Header6</h6><p>paragraph</p><pre>" +
                "pre</pre><p> <span>span</span> <strong>strong text</strong> <i>italic text</i> <b>bold text</b></p><p>" +
                " <a href=\"/about\">About</a> <a href=\"/contact\">Contact us</a></p></body></html>";

        server1 = HttpServer.create(new InetSocketAddress(62097), 0);
        server1.createContext("/test", httpExchange -> {
            httpExchange.sendResponseHeaders(200, data.length());
            OutputStream os = httpExchange.getResponseBody();
            os.write(data.toString().getBytes());
            os.close();
        });
        server1.start();
    }

    @Before
    public void initServer2() throws IOException {
        String data = "<!DOCTYPE html><html lang=\"en\"><head><meta charset=\"UTF-8\"><title>Test page Title</title>" +
                " <meta name=\"title\" content=\"Test football meta tag\"/> <meta name=\"description\" content=\"sport" +
                " description tag\"/> <meta name=\"keywords\" content=\"test, java, junit\"></head><body><h1>Header1</h1>" +
                "<h2>Header2</h2><h3>Header3</h3><h4>Header4</h4><h5>Header5</h5><h6>Header6</h6><p>paragraph</p><pre>" +
                "pre</pre><p> <span>span</span> <strong>strong text</strong> <i>italic text</i> <b>bold text</b></p><p>" +
                " <a href=\"/about\">About</a> <a href=\"/contact\">Contact us</a></p></body></html>";

        server2 = HttpServer.create(new InetSocketAddress(62098), 0);
        server2.createContext("/test", httpExchange -> {
            httpExchange.sendResponseHeaders(200, data.length());
            OutputStream os = httpExchange.getResponseBody();
            os.write(data.toString().getBytes());
            os.close();
        });
        server2.start();
    }

    @After
    public void closeServer1() {
        server1.stop(0);
    }

    @After
    public void closeServer2() {
        server2.stop(0);
    }

    @Test
    public void checkAnchorKeyWord() {
        Set<String> anchorKeyWords = new HashSet<>();
        anchorKeyWords.add("sport");
        anchorKeyWords.add("football");

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