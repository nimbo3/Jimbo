package ir.jimbo.crawler;

import ir.jimbo.commons.config.MetricConfiguration;
import ir.jimbo.crawler.config.KafkaConfiguration;
import ir.jimbo.crawler.config.RedisConfiguration;
import ir.jimbo.crawler.service.CacheService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import redis.embedded.RedisServer;

import java.io.IOException;
import java.util.Collections;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CountDownLatch;

public class LinkConsumerTest {

    private RedisServer redisServer;
    private CacheService cacheService;

    @Before
    public void setUp() throws IOException {
        MetricConfiguration metrics = new MetricConfiguration();
        redisServer = new RedisServer(6380);
        redisServer.start();
        RedisConfiguration redisConfiguration = new RedisConfiguration();
        cacheService = new CacheService(redisConfiguration, metrics.getProperty("crawler.redis.health.name"));
    }

    @After
    public void close() {
        redisServer.stop();
    }

    @Test
    public void run() throws IOException, InterruptedException {
        ArrayBlockingQueue<String> queue = new ArrayBlockingQueue<>(2);
        LinkConsumer consumer = new LinkConsumer(new KafkaConfiguration(),
                cacheService,
                new CountDownLatch(1),
                queue,
                new MetricConfiguration());
        MockConsumer<Long, String> kafkaConsumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
        kafkaConsumer.subscribe(Collections.singletonList("testTopic"));
        consumer.setConsumer(kafkaConsumer);
        kafkaConsumer.rebalance(
                Collections.singleton(new TopicPartition("testTopic", 0)));
        kafkaConsumer.seek(new TopicPartition("testTopic", 0), 0);
        kafkaConsumer.addRecord(new ConsumerRecord<>(
                "testTopic", 0, 0, 1L, "https://stackoverflow.com"));
        new Thread(() -> {
            try {
                Thread.sleep(3000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            consumer.close();
        }).start();
        consumer.run();
        String link = queue.take();
        Assert.assertEquals(link, "https://stackoverflow.com");
    }
}