package ir.jimbo;

import ir.jimbo.config.AppConfig;
import ir.jimbo.config.KafkaConfiguration;
import ir.jimbo.crawler.LinkConsumer;
import ir.jimbo.crawler.exceptions.NoDomainFoundException;
import ir.jimbo.model.Link;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.IOException;
import java.time.Duration;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

class Shuffler {

    private KafkaConfiguration kafkaConfiguration;
    private AppConfig appConfig;
    private boolean repeat;
    private Consumer<Long, String> linkConsumer;
    private Producer<Long, String> linkProducer;
    private Pattern domainPattern = Pattern.compile("^(([^:/?#]+):)?(//([^/?#]*))?([^?#]*)(\\?([^#]*))?(#(.*))?");
    // Regex pattern to extract domain from URL
    // Please refer to RFC 3986 - Appendix B for more information

    Shuffler() throws IOException {
        kafkaConfiguration = new KafkaConfiguration();
        appConfig = new AppConfig();
    }

    void start() {
        repeat = true;
        linkConsumer = kafkaConfiguration.getConsumer();
        linkProducer = kafkaConfiguration.getLinkProducer();
        Integer size;
        while (repeat) {
            size = 0;
            List<Link> links = consumeLinks(size);
            sortLinks(links);
            produceLink(links, size);
        }
    }

    private List<Link> consumeLinks(Integer size) {
        int attempt = 0;
        String url;
        List<Link> links = new ArrayList<>();
        while (size < appConfig.getLinksPerProcessSize()) {
            ConsumerRecords<Long, String> consumerRecords = linkConsumer.poll(Duration.ofMillis(kafkaConfiguration.getPollDuration()));
            size += consumerRecords.count();
            for (ConsumerRecord<Long, String> consumerRecord : consumerRecords) {
                url = consumerRecord.value();
                links.add(new Link(getDomain(url), url));
            }
            attempt ++;
            if (attempt > appConfig.getPollAttempts()) {
                break;
            }
        }
        return links;
    }

    private void sortLinks(List<Link> links) {
        links.sort(Comparator.comparing(Link::getDomain));
    }

    private void produceLink(List<Link> links, int size) {

    }

    private void sendLink(String link) {
        ProducerRecord<Long, String> record = new ProducerRecord<>(kafkaConfiguration.getLinkTopicName(), link);
        linkProducer.send(record);
    }

    String getDomain(String url) {
        final Matcher matcher = domainPattern.matcher(url);
        String result = null;
        if (matcher.matches())
            result = matcher.group(4);
        if (result == null) {
            throw new NoDomainFoundException();
        }

        if (result.startsWith("www.")) {
            result = result.substring(4);
        }
        if (result.isEmpty()) {
            throw new NoDomainFoundException();
        }
        return result;
    }

    public void close() {
        repeat = false;
    }
}
