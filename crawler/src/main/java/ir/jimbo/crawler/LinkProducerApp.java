package ir.jimbo.crawler;

import ir.jimbo.crawler.config.KafkaConfiguration;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.IOException;
import java.util.Scanner;

public class LinkProducerApp {

    public static KafkaConfiguration kafkaConfiguration;

    public static void main(String[] args) throws IOException {
        kafkaConfiguration = new KafkaConfiguration();
        Scanner scanner = new Scanner(System.in);
        Producer<Long, String> linkProducer = kafkaConfiguration.getLinkProducer();
        System.out.println("please type links:");
        while (true) {
            System.out.print(">");
            String uri = scanner.nextLine();
            if (uri.equals("exit")) {
                break;
            }
            sendUriToKafka(uri, linkProducer);
            try {
                Thread.sleep(20);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    private static void sendUriToKafka(String uri, Producer<Long, String> producer) {
        ProducerRecord<Long, String> producerRecord = new ProducerRecord<>(
                kafkaConfiguration.getLinkTopicName(), uri);
        producer.send(producerRecord);
    }
}
