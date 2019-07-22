package ir.jimbo.crawler;

import com.sun.net.httpserver.HttpServer;
import org.junit.Before;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;

public class PageParserTest {
    @Before
    public void runServer() throws IOException {
        HttpServer server = HttpServer.create(new InetSocketAddress(9898), 0);
        server.createContext("/meta_test", httpExchange -> {
            String response = "This is the response";
            httpExchange.sendResponseHeaders(200, response.length());
            OutputStream os = httpExchange.getResponseBody();
            os.write(response.getBytes());
            os.close();
        });
        server.start();
    }
}
