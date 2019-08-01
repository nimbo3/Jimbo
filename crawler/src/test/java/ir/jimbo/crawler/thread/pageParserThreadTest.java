package ir.jimbo.crawler.thread;

import com.sun.net.httpserver.HttpServer;
import ir.jimbo.commons.config.MetricConfiguration;
import ir.jimbo.commons.model.HtmlTag;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertTrue;

public class pageParserThreadTest {
    private HttpServer server;

    @Before
    public void startServer() throws IOException {
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
    public void testTitle() throws IOException {
        PageParserThread pageParser = new PageParserThread(new MetricConfiguration().getNewTimer("crawlParseTimer"));
        PageParserThread.PagePair parsed = pageParser.parse("http://localhost:9898/test");
        assertEquals("Test page Title", parsed.getElasticPage().getTitle());
    }

    @Test
    public void testH1() throws IOException {
        PageParserThread pageParser = new PageParserThread(new MetricConfiguration().getNewTimer("crawlParseTimer"));
        PageParserThread.PagePair parsed = pageParser.parse("http://localhost:9898/test");
        assertEquals(1, parsed.getElasticPage().getH1List().size());
        assertEquals("Header1", parsed.getElasticPage().getH1List().get(0).getContent());
    }

    @Test
    public void testH2() throws IOException {
        PageParserThread pageParser = new PageParserThread(new MetricConfiguration().getNewTimer("crawlParseTimer"));
        PageParserThread.PagePair parsed = pageParser.parse("http://localhost:9898/test");
        assertEquals(1, parsed.getElasticPage().getH2List().size());
        assertEquals("Header2", parsed.getElasticPage().getH2List().get(0).getContent());
    }

    @Test
    public void testH3to6() throws IOException {
        PageParserThread pageParser = new PageParserThread(new MetricConfiguration().getNewTimer("crawlParseTimer"));
        PageParserThread.PagePair parsed = pageParser.parse("http://localhost:9898/test");
        assertEquals(4, parsed.getElasticPage().getH3to6List().size());
        assertTrue(parsed.getElasticPage().getH3to6List().contains(new HtmlTag("h3", "Header3")));
        assertTrue(parsed.getElasticPage().getH3to6List().contains(new HtmlTag("h4", "Header4")));
        assertTrue(parsed.getElasticPage().getH3to6List().contains(new HtmlTag("h5", "Header5")));
        assertTrue(parsed.getElasticPage().getH3to6List().contains(new HtmlTag("h6", "Header6")));
    }

    @Test
    public void testPlainText() throws IOException {
        PageParserThread pageParser = new PageParserThread(new MetricConfiguration().getNewTimer("crawlParseTimer"));
        PageParserThread.PagePair parsed = pageParser.parse("http://localhost:9898/test");
        assertTrue(parsed.getElasticPage().getPlainTextList().contains(new HtmlTag("p", "paragraph")));
        assertTrue(parsed.getElasticPage().getPlainTextList().contains(new HtmlTag("pre", "pre")));
        assertTrue(parsed.getElasticPage().getPlainTextList().contains(new HtmlTag("span", "span")));
        assertTrue(parsed.getElasticPage().getPlainTextList().contains(new HtmlTag("p", "span strong text italic text bold text")));
        assertTrue(parsed.getElasticPage().getPlainTextList().contains(new HtmlTag("p", "About Contact us")));
    }

    @Test
    public void testLinks() throws IOException {
        PageParserThread pageParser = new PageParserThread(new MetricConfiguration().getNewTimer("crawlParseTimer"));
        PageParserThread.PagePair parsed = pageParser.parse("http://localhost:9898/test");
        HtmlTag aboutTag = new HtmlTag("a", "About");
        HtmlTag contactUsTag = new HtmlTag("a", "Contact us");
        assertTrue(parsed.getHBasePage().getLinks().contains(aboutTag));
        assertTrue(parsed.getHBasePage().getLinks().contains(contactUsTag));
        if (parsed.getHBasePage().getLinks().get(0).getContent().equals("About"))
            assertEquals(parsed.getHBasePage().getLinks().get(0).getProps().get("href"), "http://localhost:9898/about");
        else
            assertEquals(parsed.getHBasePage().getLinks().get(0).getProps().get("href"), "http://localhost:9898/contact");
        if (parsed.getHBasePage().getLinks().get(0).getContent().equals("Contact us"))
            assertEquals(parsed.getHBasePage().getLinks().get(0).getProps().get("href"), "http://localhost:9898/contact");
        else
            assertEquals(parsed.getHBasePage().getLinks().get(0).getProps().get("href"), "http://localhost:9898/about");
    }

    @After
    public void stopServer() {
        server.stop(0);
    }
}
