package in.nimbo.jimbo.parse;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;

public class PageParser implements Runnable{

    private String url;

    PageParser(String url) {
        this.url = url;
    }

    @Override
    public void run() {
        Document document = Jsoup.parse(url);
    }
}
