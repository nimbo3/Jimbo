package ir.jimbo.crawler;

import ir.jimbo.commons.config.MetricConfiguration;
import ir.jimbo.crawler.config.KafkaConfiguration;
import ir.jimbo.crawler.config.RedisConfiguration;
import ir.jimbo.crawler.exceptions.NoDomainFoundException;
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
        MetricConfiguration metrics = MetricConfiguration.getInstance();
        redisServer = new RedisServer(6380);
        redisServer.start();
        RedisConfiguration redisConfiguration = new RedisConfiguration();
        cacheService = new CacheService(redisConfiguration);
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
                MetricConfiguration.getInstance());
        MockConsumer<Long, String> kafkaConsumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
        kafkaConsumer.subscribe(Collections.singletonList("testTopic"));
        consumer.setConsumer(kafkaConsumer);
        kafkaConsumer.rebalance(
                Collections.singleton(new TopicPartition("testTopic", 0)));
        kafkaConsumer.seek(new TopicPartition("testTopic", 0), 0);
        kafkaConsumer.addRecord(new ConsumerRecord<>(
                "testTopic", 0, 0, 1L, "https://stackoverflow.com"));
        kafkaConsumer.addRecord(new ConsumerRecord<>(
                "testTopic", 0, 0, 2L, "https://ssdftackoverflow.com"));
        new Thread(() -> {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            } finally {
                consumer.interrupt();
            }
        }).start();
        System.err.println("before running consumer");
        consumer.run();
        System.err.println("before add to queue");
        String link = queue.take();
        System.err.println("after getting from queue");
        Assert.assertEquals(link, "https://stackoverflow.com");
    }

    @Test
    public void getDomain() throws IOException {
        LinkConsumer consumer = new LinkConsumer(new KafkaConfiguration(),
                cacheService,
                new CountDownLatch(3),
                new ArrayBlockingQueue<>(3),
                MetricConfiguration.getInstance());
        Assert.assertEquals(consumer.getDomain("http://stackoverflow.com/"), "stackoverflow.com");
        Assert.assertEquals(consumer.getDomain("https://stackoverflow.com/"), "stackoverflow.com");
        Assert.assertEquals(consumer.getDomain("http://stackoverflow.com"), "stackoverflow.com");
        Assert.assertEquals(consumer.getDomain("https://stackoverflow.com/"), "stackoverflow.com");
        Assert.assertEquals(consumer.getDomain("https://stackoverflow.co.io/"), "stackoverflow.co.io");
        Assert.assertEquals(consumer.getDomain("https://disscus.stackoverflow.co.io"), "disscus.stackoverflow.co.io");
        Assert.assertEquals(consumer.getDomain("https://www.disscus.stackoverflow.co.io"), "disscus.stackoverflow.co.io");
        String str1 = consumer.getDomain("http://www.discuss.stackoverflow.co.io");
        String str2 = consumer.getDomain("http://discuss.stackoverflow.co.io");
        Assert.assertEquals(str1, str2);
    }

    /**
     * Uris that do not start with "http://" or "https://" must be ignored. according to pattern these
     *  uris pattern are null
     * @throws IOException
     */
    @Test (expected = NoDomainFoundException.class)
    public void getDomainBadUri1() throws IOException {
        LinkConsumer consumer = new LinkConsumer(new KafkaConfiguration(),
                cacheService,
                new CountDownLatch(3),
                new ArrayBlockingQueue<>(3),
                MetricConfiguration.getInstance());
        consumer.getDomain("hello.com");
    }

    /**
     * Uris that do not start with "http://" or "https://" must be ignored. according to pattern these
     *  uris pattern are null. because jSoup can not fetch these uris.
     * @throws IOException
     */
    @Test (expected = NoDomainFoundException.class)
    public void getDomainBadUri2() throws IOException {
        LinkConsumer consumer = new LinkConsumer(new KafkaConfiguration(),
                cacheService,
                new CountDownLatch(3),
                new ArrayBlockingQueue<>(3),
                MetricConfiguration.getInstance());
        consumer.getDomain("www.hello.com");
    }

    /**
     * Empty domains must be ignored.
     * @throws IOException
     */
    @Test (expected = NoDomainFoundException.class)
    public void getDomainBadUri3() throws IOException {
        LinkConsumer consumer = new LinkConsumer(new KafkaConfiguration(),
                cacheService,
                new CountDownLatch(3),
                new ArrayBlockingQueue<>(3),
                MetricConfiguration.getInstance());
        consumer.getDomain("http://www.");
    }
}