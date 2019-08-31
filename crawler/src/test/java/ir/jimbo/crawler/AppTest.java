package ir.jimbo.crawler;


import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

public class AppTest {


    @Test
    public void initConfigsWithoutPath() throws IOException {
        App.initializeConfigurations(new String[0]);
        Assert.assertEquals(App.getAppConfiguration().getProperty("initialize.test"), "jimbo");
    }

    @Test
    public void initConfigsWithPath() throws IOException {
        String[] args = new String[3];
        args[0] = "app:src/test/resources/appConfigClone.properties";
        args[1] = "redis:src/test/resources/redisConfigClone.properties";
        args[2] = "kafka:src/test/resources/kafkaConfigClone.properties";
        App.initializeConfigurations(args);
        Assert.assertEquals(App.getAppConfiguration().getProperty("initialize.test"), "jimbo");
        Assert.assertEquals(App.getRedisConfiguration().getPassword(), "jimbo");
        Assert.assertEquals(App.getKafkaConfiguration().getPollDuration(), 0);
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
        App.setConsumers(new Thread[3]);
        for (int i = 0; i < 3; i++) {
            App.getConsumers()[i] = new InfiniteThread();
        }
        Assert.assertEquals(App.getAllWakeConsumers(), 0);
        for (int i = 0; i < 3; i++) {
            App.getConsumers()[i].start();
        }
        Assert.assertEquals(App.getAllWakeConsumers(), 3);
        App.getConsumers()[2].interrupt();
        Thread.sleep(1100);
        Assert.assertEquals(App.getAllWakeConsumers(), 2);
        App.getConsumers()[1].interrupt();
        Thread.sleep(1100);
        Assert.assertEquals(App.getAllWakeConsumers(), 1);
        App.getConsumers()[0].interrupt();
        Thread.sleep(1100);
        Assert.assertEquals(App.getAllWakeConsumers(), 0);
    }

    @Test
    public void awakeProducers() throws InterruptedException {
        App.setProducers(new Thread[3]);
        for (int i = 0; i < 3; i++) {
            App.getProducers()[i] = new InfiniteThread();
        }
        Assert.assertEquals(App.getAllWakeProducers(), 0);
        for (int i = 0; i < 3; i++) {
            App.getProducers()[i].start();
        }
        Thread.sleep(1000);
        Assert.assertEquals(App.getAllWakeProducers(), 3);
        App.getProducers()[2].interrupt();
        Thread.sleep(1100);
        Assert.assertEquals(App.getAllWakeProducers(), 2);
        App.getProducers()[1].interrupt();
        Thread.sleep(1100);
        Assert.assertEquals(App.getAllWakeProducers(), 1);
        App.getProducers()[0].interrupt();
        Thread.sleep(1100);
        Assert.assertEquals(App.getAllWakeProducers(), 0);
    }

    private class InfiniteThread extends Thread {

        AtomicBoolean repeat;

        InfiniteThread() {
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