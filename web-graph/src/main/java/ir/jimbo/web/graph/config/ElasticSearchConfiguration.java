package ir.jimbo.web.graph.config;

import lombok.Getter;
import lombok.Setter;
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

@Getter
@Setter
public class ElasticSearchConfiguration extends Config {
    private static final Logger LOGGER = LogManager.getLogger(ElasticSearchConfiguration.class);
    private static final String PREFIX = "elasticsearch";
    private static ElasticSearchConfiguration instance = null;

    private List<String> urls;
    private String sourceName;
    private String indexName;
    private String clusterName;
    private TransportClient client;

    private int requestTimeOutNanos;
    private int numberOfRetry;

    public static ElasticSearchConfiguration getInstance() throws IOException {
        if (instance == null)
            instance = new ElasticSearchConfiguration();
        return instance;
    }

    private ElasticSearchConfiguration() throws IOException {
        super(PREFIX);
        requestTimeOutNanos = Integer.parseInt(getPropertyValue("request.timeout"));
        urls = Arrays.asList(getPropertyValue("nodes.url").split(","));
        sourceName = getPropertyValue("source.name");
        indexName = getPropertyValue("index.name");
        clusterName = getPropertyValue("cluster.name");
        numberOfRetry = Integer.parseInt(getPropertyValue("retry.number"));
    }

    public TransportClient getClient() {
        if (client == null) {
            Settings settings = Settings.builder().put("cluster.name", clusterName).build();
            client = new PreBuiltTransportClient(settings);
            for (String url : urls) {
                String[] urlAndPort = url.split(":");
                try {
                    client.addTransportAddress(new TransportAddress(Inet4Address.getByName(urlAndPort[0])
                            , Integer.parseInt(urlAndPort[1])));
                } catch (UnknownHostException e) {
                    LOGGER.error("elasticsearch node with url: {} does't exist", urlAndPort, e);
                }
            }
        }
        return client;
    }

}
