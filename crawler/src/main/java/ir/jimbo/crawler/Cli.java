package ir.jimbo.crawler;

import ir.jimbo.crawler.config.KafkaConfiguration;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Scanner;

public class Cli extends Thread {

    private Producer<Long, String> producer;
    private String topicName;

    Cli(KafkaConfiguration kafkaConfiguration) {
        this.producer = kafkaConfiguration.getLinkProducer();
        this.topicName = kafkaConfiguration.getProperty("links.topic.name");
    }

    @Override
    public void run() {
        Scanner jin = new Scanner(System.in);
        while (true) {
            System.out.println("enter url to add to kafka : ");
            ProducerRecord<Long, String> record = new ProducerRecord<>(topicName, jin.nextLine());
            producer.send(record);
        }
    }
}
