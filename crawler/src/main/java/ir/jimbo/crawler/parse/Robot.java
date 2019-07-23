package ir.jimbo.crawler.parse;

import com.panforge.robotstxt.RobotsTxt;

import java.io.Serializable;
import java.util.List;

public class Robot implements RobotsTxt, Serializable {


    @Override
    public boolean query(String userAgent, String path) {
        return false;
    }

    @Override
    public Integer getCrawlDelay() {
        return null;
    }

    @Override
    public String getHost() {
        return null;
    }

    @Override
    public List<String> getSitemaps() {
        return null;
    }

    @Override
    public List<String> getDisallowList(String userAgent) {
        return null;
    }
}
