package ir.jimbo.crawler;

import ir.jimbo.crawler.kafka.PageAndLinkProducer;

import java.util.Scanner;

public class Cli extends Thread {

    PageAndLinkProducer producer;
    String topicName;

    Cli(PageAndLinkProducer producer, String topicName) {
        this.producer = producer;
        this.topicName = topicName;
    }

    @Override
    public void run() {
        Scanner jin = new Scanner(System.in);
        while (true) {
            System.out.println("enter url to add to kafka : ");
            producer.addLinkToKafka(topicName, jin.nextLine());
        }
    }
}
