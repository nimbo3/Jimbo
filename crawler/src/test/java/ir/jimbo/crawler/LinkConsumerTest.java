package ir.jimbo.crawler;

import ir.jimbo.commons.config.MetricConfiguration;
import ir.jimbo.crawler.config.KafkaConfiguration;
import ir.jimbo.crawler.service.CacheService;
import org.apache.logging.log4j.Logger;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import sun.nio.ch.Interruptible;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.mockito.Mockito.*;

public class LinkConsumerTest {
    @Mock
    Logger logger;
    @Mock
    KafkaConfiguration kafkaConfiguration;
    @Mock
    CacheService cacheService;
    @Mock
    AtomicBoolean repeat;
    @Mock
    CountDownLatch countDownLatch;
    @Mock
    MetricConfiguration metrics;
    //Field domainPattern of type Pattern - was not mocked since Mockito doesn't mock a Final class when 'mock-maker-inline' option is not set
    @Mock
    Thread threadQ;
    @Mock
    Runnable target;
    @Mock
    ThreadGroup group;
    @Mock
    ClassLoader contextClassLoader;
    //Field inheritedAccessControlContext of type AccessControlContext - was not mocked since Mockito doesn't mock a Final class when 'mock-maker-inline' option is not set
    @Mock
    ThreadLocal.ThreadLocalMap threadLocals;
    @Mock
    ThreadLocal.ThreadLocalMap inheritableThreadLocals;
    @Mock
    Object parkBlocker;
    @Mock
    Interruptible blocker;
    @Mock
    Object blockerLock;
    @Mock
    Thread.UncaughtExceptionHandler uncaughtExceptionHandler;
    @Mock
    Thread.UncaughtExceptionHandler defaultUncaughtExceptionHandler;
    @InjectMocks
    LinkConsumer linkConsumer;

    @Before
    public void setUp() {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testRun() throws Exception {


        when(kafkaConfiguration.getLinkTopicName()).thenReturn("getLinkTopicNameResponse");
        when(kafkaConfiguration.getLinkProducer()).thenReturn(null);
        when(kafkaConfiguration.getConsumer()).thenReturn(null);
        when(cacheService.isDomainExist(anyString())).thenReturn(true);
        when(cacheService.isUrlExists(anyString())).thenReturn(true);
        when(metrics.getNewTimer(anyString())).thenReturn(null);
        when(metrics.getProperty(anyString())).thenReturn("getPropertyResponse");

        linkConsumer.start();
    }

    @Test
    public void testClose() throws Exception {
        linkConsumer.close();
    }
}

//Generated with love by TestMe :) Please report issues and submit feature requests at: http://weirddev.com/forum#!/testme