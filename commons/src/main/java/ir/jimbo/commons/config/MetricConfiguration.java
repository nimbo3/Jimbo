package ir.jimbo.commons.config;

import com.codahale.metrics.*;
import com.codahale.metrics.graphite.Graphite;
import com.codahale.metrics.graphite.GraphiteReporter;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class MetricConfiguration {
    private MetricRegistry metricRegistry;
    private Graphite graphite;
    private int graphitePort = 2003;
    private String graphiteUrl;
    private Properties properties;

    public MetricConfiguration() throws IOException {
        properties = new Properties();
        properties.load(Objects.requireNonNull(Thread.currentThread().getContextClassLoader()
                .getResourceAsStream("metric.properties")));
        metricRegistry = new MetricRegistry();
        graphitePort = Integer.parseInt(properties.getProperty("graphite.port"));
        graphiteUrl = properties.getProperty("graphite.url");
        connectToReporter();
    }

    public Meter getNewMeter(String name) {
        return metricRegistry.meter(name);
    }

    public Histogram getNewHistogram(String name) {
        return metricRegistry.histogram(name);
    }

    public Counter getNewCounter(String name) {
        return metricRegistry.counter(name);
    }

    public Timer getNewTimer(String name) {
        return metricRegistry.timer(name);
    }

    private void connectToReporter() {
        graphite = new Graphite(new InetSocketAddress(graphiteUrl, graphitePort));
        final GraphiteReporter reporter = GraphiteReporter.forRegistry(metricRegistry)
                .prefixedWith("web1.example.com")
                .convertRatesTo(TimeUnit.SECONDS)
                .convertDurationsTo(TimeUnit.MILLISECONDS)
                .filter(MetricFilter.ALL)
                .build(graphite);
        reporter.start(1, TimeUnit.MINUTES);
    }

    public String getProperty(String key) {
        return properties.getProperty(key);
    }
}
