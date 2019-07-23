package ir.jimbo.crawler.parse;

import me.jamesfrost.robotsio.RobotsDisallowedException;
import me.jamesfrost.robotsio.RobotsParser;

import java.net.MalformedURLException;

public class RobotsTxtParser {
    private String url;

    RobotsTxtParser(String url) {
        this.url = url;
    }

    private static RobotsParser allowAll = new RobotsParser();

    public RobotsParser parse() {
        RobotsParser robotsParser = allowAll;
        try {
            robotsParser = new RobotsParser();
            robotsParser.connect(url);
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
