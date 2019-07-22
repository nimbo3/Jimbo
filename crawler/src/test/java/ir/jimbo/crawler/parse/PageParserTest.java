package ir.jimbo.crawler.parse;

import com.sun.net.httpserver.HttpServer;
import ir.jimbo.commons.model.Page;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertTrue;

public class PageParserTest {
    private HttpServer server;

    @Before
    public void startServer() throws IOException {
        // Getting simple page content
        String data = "<!DOCTYPE html><html lang=\"en\"><head><meta charset=\"UTF-8\"><title>Test page Title</title> <meta name=\"title\" content=\"Test page meta tag\"/> <meta name=\"description\" content=\"Test page description tag\"/> <meta name=\"keywords\" content=\"test, java, junit\"></head><body><h1>Header1</h1><h2>Header2</h2><h3>Header3</h3><h4>Header4</h4><h5>Header5</h5><h6>Header6</h6><p>paragraph</p><pre>pre</pre><p> <span>span</span> <strong>strong text</strong> <i>italic text</i> <b>bold text</b></p><p> <a href=\"/about\">About</a> <a href=\"/contact\">Contact us</a></p></body></html>";

        // Starting http server
        server = HttpServer.create(new InetSocketAddress(9898), 0);
        server.createContext("/test", httpExchange -> {
            httpExchange.sendResponseHeaders(200, data.length());
            OutputStream os = httpExchange.getResponseBody();
            os.write(data.toString().getBytes());
            os.close();
        });
        server.start();
    }

    @Test
    public void testTitle() {
        PageParser pageParser = new PageParser("http://localhost:9898/test");
        Page page = pageParser.parse();
        assertEquals("Test page Title", page.getTitle());
    }

    @Test
    public void testH1() {
        PageParser pageParser = new PageParser("http://localhost:9898/test");
        Page page = pageParser.parse();
        assertEquals(1, page.getH1List().size());
        assertEquals("Header1", page.getH1List().get(0));
    }

    @Test
    public void testH2() {
        PageParser pageParser = new PageParser("http://localhost:9898/test");
        Page page = pageParser.parse();
        assertEquals(1, page.getH2List().size());
        assertEquals("Header2", page.getH2List().get(0));
    }

    @Test
    public void testH3to6() {
        PageParser pageParser = new PageParser("http://localhost:9898/test");
        Page page = pageParser.parse();
        assertEquals(4, page.getH3to6List().size());
        assertTrue(page.getH3to6List().contains("Header3"));
        assertTrue(page.getH3to6List().contains("Header4"));
        assertTrue(page.getH3to6List().contains("Header5"));
        assertTrue(page.getH3to6List().contains("Header6"));
    }

    @Test
    public void testPlainText() {
        PageParser pageParser = new PageParser("http://localhost:9898/test");
        Page page = pageParser.parse();
        assertEquals(5, page.getPlainTextList().size());
        assertTrue(page.getPlainTextList().contains("paragraph"));
        assertTrue(page.getPlainTextList().contains("pre"));
        assertTrue(page.getPlainTextList().contains("span"));
        assertTrue(page.getPlainTextList().contains("span strong text italic text bold text"));
        assertTrue(page.getPlainTextList().contains("About Contact us"));
    }

    @Test
    public void testLinks() {
        PageParser pageParser = new PageParser("http://localhost:9898/test");
        Page page = pageParser.parse();
        assertEquals(2, page.getLinks().size());
        assertTrue(page.getLinks().containsKey("About"));
        assertTrue(page.getLinks().containsKey("Contact us"));
        assertEquals(page.getLinks().get("About"), "http://localhost:9898/about");
        assertEquals(page.getLinks().get("Contact us"), "http://localhost:9898/contact");
    }

    @After
    public void stopServer() {
        server.stop(0);
    }
}
