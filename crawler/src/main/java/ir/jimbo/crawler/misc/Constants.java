package ir.jimbo.crawler.misc;

public class Constants {

    public static String KAFKA_LINKS_TOPIC = "links";
    public static String KAFKA_PAGES_TOPIC = "pages";

    public Constants(String kafkaLinksTopic, String kafkaPagesTopic) {
        KAFKA_LINKS_TOPIC = kafkaLinksTopic;
        KAFKA_PAGES_TOPIC = kafkaPagesTopic;
    }
}
