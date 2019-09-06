package ir.jimbo.commons.config;


import com.codahale.metrics.*;
import com.codahale.metrics.jmx.JmxReporter;

import java.io.IOException;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class MetricConfiguration {

    private static MetricConfiguration metricConfiguration;
    private MetricRegistry metricRegistry;
    private Properties properties;

    private MetricConfiguration() throws IOException {
        properties = new Properties();
        properties.load(Objects.requireNonNull(Thread.currentThread().getContextClassLoader()
                .getResourceAsStream("metric.properties")));
        connectToReporter();
    }

    public static MetricConfiguration getInstance() throws IOException {
        if (metricConfiguration == null) {
            metricConfiguration = new MetricConfiguration();
            return metricConfiguration;
        } else {
            return metricConfiguration;
        }
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
        SharedMetricRegistries.setDefault("joojle");
        metricRegistry = SharedMetricRegistries.getDefault();
        final JmxReporter reporter = JmxReporter.forRegistry(metricRegistry)
                .convertDurationsTo(TimeUnit.MILLISECONDS)
                .convertRatesTo(TimeUnit.SECONDS)
                .filter(MetricFilter.ALL)
                .build();
        reporter.start();
    }

    public String getProperty(String key) {
        return properties.getProperty(key);
    }
}
