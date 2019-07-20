package in.nimbo.jimbo;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;

public class Parser implements Runnable{

    private String url;

    Parser(String url) {
        this.url = url;
    }

    @Override
    public void run() {
        Document document = Jsoup.parse(url);
    }
}
