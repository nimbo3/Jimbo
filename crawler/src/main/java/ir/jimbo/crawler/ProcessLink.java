package ir.jimbo.crawler;

public class ProcessLink extends Thread{

    String title;
    String url;

    public ProcessLink(String title, String url) {
        this.title = title;
        this.url = url;
    }

}
