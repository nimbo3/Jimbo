package ir.jimbo.crawler;


import com.codahale.metrics.Counter;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

public class AppTest {


    @Test
    public void initConfigsWithoutPath() throws IOException {
        App.initializeConfigurations(new String[0]);
        Assert.assertEquals(App.appConfiguration.getProperty("initialize.test"), "jimbo");
    }

    @Test
    public void initConfigsWithPath() throws IOException {
        String[] args = new String[3];
        args[0] = "app:src/test/resources/appConfigClone.properties";
        args[1] = "redis:src/test/resources/redisConfigClone.properties";
        args[2] = "kafka:src/test/resources/kafkaConfigClone.properties";
        App.initializeConfigurations(args);
        Assert.assertEquals(App.appConfiguration.getProperty("initialize.test"), "jimbo");
        Assert.assertEquals(App.redisConfiguration.getPassword(), "jimbo");
        Assert.assertEquals(App.kafkaConfiguration.getPollDuration(), 0);
    }

    @Test (expected = IOException.class)
    public void initConfigsWithBadPath() throws IOException {
        String[] args = new String[3];
        args[0] = "app:src/test/appConfigClone.properties";
        args[1] = "redis:src/test/resources/redisConfigClone.properties";
        args[2] = "kafka:src/test/resources/kafkaConfigClone.properties";
        App.initializeConfigurations(args);
    }

    @Test
    public void awakeConsumer() throws InterruptedException {
        App.consumers = new Thread[3];
        for (int i = 0; i < 3; i++) {
            App.consumers[i] = new InfinitThread();
        }
        Assert.assertEquals(App.getAllWakeConsumers(new Counter()), 0);
        for (int i = 0; i < 3; i++) {
            App.consumers[i].start();
        }
        Assert.assertEquals(App.getAllWakeConsumers(new Counter()), 3);
        App.consumers[2].interrupt();
        Thread.sleep(1100);
        Assert.assertEquals(App.getAllWakeConsumers(new Counter()), 2);
        App.consumers[1].interrupt();
        Thread.sleep(1100);
        Assert.assertEquals(App.getAllWakeConsumers(new Counter()), 1);
        App.consumers[0].interrupt();
        Thread.sleep(1100);
        Assert.assertEquals(App.getAllWakeConsumers(new Counter()), 0);
    }

    @Test
    public void awakeProducers() throws InterruptedException {
        App.producers = new Thread[3];
        for (int i = 0; i < 3; i++) {
            App.producers[i] = new InfinitThread();
        }
        Assert.assertEquals(App.getAllWakeConsumers(new Counter()), 0);
        for (int i = 0; i < 3; i++) {
            App.producers[i].start();
        }
        Thread.sleep(1000);
        Assert.assertEquals(App.getAllWakeProducers(new Counter()), 3);
        App.producers[2].interrupt();
        Thread.sleep(1100);
        Assert.assertEquals(App.getAllWakeProducers(new Counter()), 2);
        App.producers[1].interrupt();
        Thread.sleep(1100);
        Assert.assertEquals(App.getAllWakeProducers(new Counter()), 1);
        App.producers[0].interrupt();
        Thread.sleep(1100);
        Assert.assertEquals(App.getAllWakeProducers(new Counter()), 0);
    }

    private class InfinitThread extends Thread {

        AtomicBoolean repeat;

        InfinitThread() {
            repeat = new AtomicBoolean(true);
        }

        @Override
        public void run() {
            while (repeat.get()) {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    //
                }
            }
        }

        @Override
        public void interrupt() {
            repeat.set(false);
        }
    }
}