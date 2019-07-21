package ir.jimbo.crawler.parse;

import com.panforge.robotstxt.RobotsTxt;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Collections;
import java.util.List;

public class RobotsParser {
    private String domain;

    RobotsParser(String domain) {
        this.domain = domain;
    }

    private RobotsTxt notFoundRobotsTxt = new RobotsTxt() {
        @Override
        public boolean query(String s, String s1) {
            // Allow access to all HTTP path
            return true;
        }

        @Override
        public Integer getCrawlDelay() {
            return 0;
        }

        @Override
        public String getHost() {
            return domain;
        }

        @Override
        public List<String> getSitemaps() {
            return Collections.emptyList();
        }

        @Override
        public List<String> getDisallowList(String s) {
            return null;
        }
    };

    public RobotsTxt parse() {
        RobotsTxt robotsTxt = notFoundRobotsTxt;
        try (InputStream robotsTxtStream = new URL("https://github.com/robots.txt").openStream()) {
            robotsTxt = RobotsTxt.read(robotsTxtStream);
        } catch (IOException e) {

            // TODO: save log

            e.printStackTrace();
        }

        return robotsTxt;
    }
}
