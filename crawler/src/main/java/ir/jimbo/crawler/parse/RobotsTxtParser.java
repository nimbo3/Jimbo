package ir.jimbo.crawler.parse;

import me.jamesfrost.robotsio.RobotsDisallowedException;
import me.jamesfrost.robotsio.RobotsParser;

import java.net.MalformedURLException;

public class RobotsTxtParser {
    private String domain;

    RobotsTxtParser(String domain) {
        this.domain = domain;
    }

    private static RobotsParser allowAll = new RobotsParser("Jimbo-Crawler");

    public RobotsParser parse() {
        RobotsParser robotsParser = allowAll;
        try {
            robotsParser = new RobotsParser("Jimbo-Crawler");
            robotsParser.connect(domain);
        } catch (RobotsDisallowedException e) {
            // TODO: log
            e.printStackTrace();
        } catch (MalformedURLException e) {
            // TODO: log
            e.printStackTrace();
        }
        return robotsParser;
    }
}
