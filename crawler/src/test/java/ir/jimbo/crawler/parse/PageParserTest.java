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

    @After
    public void stopServer() {
        server.stop(0);
    }
}
