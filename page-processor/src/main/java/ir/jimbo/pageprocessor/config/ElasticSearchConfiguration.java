package ir.jimbo.pageprocessor.config;

import lombok.Getter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.io.IOException;
import java.net.Inet4Address;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Properties;

@Getter
public class ElasticSearchConfiguration extends Config {
    private static final Logger LOGGER = LogManager.getLogger(HConfig.class);
    private static final String PREFIX = "elasticsearch";
    private static ElasticSearchConfiguration instance = null;

    private List<String> urls;
    private String indexName;
    private String clusterName;
    private TransportClient client;

    private Properties properties = new Properties();
    private int requestTimeOutNanos;

    public static ElasticSearchConfiguration getInstance() throws IOException {
        if (instance == null)
            instance = new ElasticSearchConfiguration();
        return instance;
    }

    private ElasticSearchConfiguration() throws IOException {
        super(PREFIX);
        properties.load(Objects.requireNonNull(Thread.currentThread().getContextClassLoader()
                .getResourceAsStream("configs.properties")));
        requestTimeOutNanos = Integer.parseInt(properties.getProperty("request.timeout"));
        urls = Arrays.asList(properties.getProperty("nodes.url").split(","));
        indexName = properties.getProperty("index.name");
        clusterName = properties.getProperty("cluster.name");
    }

    public TransportClient getClient() {
        if (client == null) {
            Settings settings = Settings.builder().put("cluster.name", clusterName).build();
            client = new PreBuiltTransportClient(settings);
            for (String url : urls) {
                String[] urlAndPort = url.split(":");
                try {
                    client.addTransportAddress(new TransportAddress(Inet4Address.getByName(urlAndPort[0]), Integer.parseInt(urlAndPort[1])));
                } catch (UnknownHostException e) {
                    LOGGER.error("elasticsearch node with url:" + url + "does't exist", e);
                }
            }
        }
        return client;
    }

    public int getRequestTimeOutNanos() {
        return requestTimeOutNanos;
    }

    public void setRequestTimeOutNanos(int requestTimeOutNanos) {
        this.requestTimeOutNanos = requestTimeOutNanos;
    }
}
