package ir.jimbo.crawler;

import com.sun.net.httpserver.HttpServer;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;

public class PageParserTest {
    private HttpServer server;

    @Before
    public void runServer() throws IOException {
        server = HttpServer.create(new InetSocketAddress(9898), 0);
        server.createContext("/meta_test", httpExchange -> {
            String response = "This is the response";
            httpExchange.sendResponseHeaders(200, response.length());
            OutputStream os = httpExchange.getResponseBody();
            os.write(response.getBytes());
            os.close();
        });
        server.start();
    }

    @After
    public void stopServer() {
        server.stop(0);
    }
}
